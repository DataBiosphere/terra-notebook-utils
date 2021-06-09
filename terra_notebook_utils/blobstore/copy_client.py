import os
import logging
import multiprocessing
from enum import Enum
from math import ceil
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Union

from getm import default_chunk_size
from getm.progress import ProgressBar, ProgressLogger
from getm.concurrent.collections import ConcurrentHeap

from terra_notebook_utils.blobstore.gs import GSBlobStore, GSBlob
from terra_notebook_utils.blobstore.url import URLBlobStore, URLBlob
from terra_notebook_utils.blobstore.local import LocalBlobStore, LocalBlob
from terra_notebook_utils.blobstore import BlobstoreChecksumError


logger = logging.getLogger(__name__)

AnyBlobStore = Union[GSBlobStore, URLBlobStore, LocalBlobStore]
AnyBlob = Union[GSBlob, URLBlob, LocalBlob]
CloudBlob = GSBlob

class ProgressIndicatorType(Enum):
    log = ProgressLogger
    bar = ProgressBar

class Progress:
    """This class provides a globally configured progress indicator type (Mixing types would produce terrible output),
    as well as a factory method for instantiating new indicators. Typically only a single 'bar' indicator should be
    active at any time. Multiple 'log' indicators may be active simultaneously.
    """
    indicator_type: ProgressIndicatorType = ProgressIndicatorType.log

    @classmethod
    def indicator(cls, name: str, size: int):
        if cls.indicator_type == ProgressIndicatorType.bar:
            incriments = 40
        elif cls.indicator_type == ProgressIndicatorType.log:
            incriments = ceil(size / default_chunk_size / 2)
        return cls.indicator_type.value(name, size, incriments)

def _download(src_blob: AnyBlob, dst_blob: LocalBlob):
    logger.debug(f"Starting download {src_blob.url} to {dst_blob.url}")
    dirname = os.path.dirname(dst_blob.url)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    # The download methods for each Blob is expected to compute checksums
    with Progress.indicator(dst_blob.url, src_blob.size()) as progress:
        for part_size in src_blob.download_iter(dst_blob.url):
            progress.add(part_size)

def _copy_intra_cloud(src_blob: AnyBlob, dst_blob: AnyBlob):
    # In general it is not necesary to compute checksums for intra cloud copies. The Storage services will do that for
    # us.
    # However, S3Etags depend on the part layout. Either ensure source and destination part layouts are the same, or
    # compute the destination S3Etag on the fly.
    logger.debug(f"Starting intra-cloud {src_blob.url} to {dst_blob.url}")
    assert isinstance(src_blob, type(dst_blob))
    with Progress.indicator(dst_blob.url, src_blob.size()) as progress:
        for part_size in dst_blob.copy_from_iter(src_blob):  # type: ignore
            progress.add(part_size)
    if src_blob.cloud_native_checksum() != dst_blob.cloud_native_checksum():
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        raise BlobstoreChecksumError()

def _copy_oneshot_passthrough(src_blob: AnyBlob, dst_blob: CloudBlob):
    logger.debug(f"Starting oneshot passthrough {src_blob.url} to {dst_blob.url}")
    with Progress.indicator(dst_blob.url, src_blob.size()) as progress:
        data = src_blob.get()
        dst_blob.put(data)
        progress.add(len(data))
    if not dst_blob.Hasher(data).matches(dst_blob.cloud_native_checksum()):
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        raise BlobstoreChecksumError()

def _copy_multipart_passthrough(src_blob: AnyBlob, dst_blob: CloudBlob):
    logger.debug(f"Starting multipart passthrough {src_blob.url} to {dst_blob.url}")
    cs = dst_blob.Hasher()
    with Progress.indicator(dst_blob.url, src_blob.size()) as progress:
        with dst_blob.multipart_writer() as writer:
            for part in src_blob.iter_content():
                writer.put_part(part)
                cs.update(part.data)
                progress.add(len(part.data))
    if not cs.matches(dst_blob.cloud_native_checksum()):
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        raise BlobstoreChecksumError()

def _do_copy(src_blob: AnyBlob, dst_blob: AnyBlob, multipart_threshold: int):
    try:
        if isinstance(dst_blob, LocalBlob):
            _download(src_blob, dst_blob)
        elif isinstance(src_blob, type(dst_blob)):
            _copy_intra_cloud(src_blob, dst_blob)
        elif isinstance(dst_blob, CloudBlob):
            if src_blob.size() <= multipart_threshold:
                _copy_oneshot_passthrough(src_blob, dst_blob)
            else:
                _copy_multipart_passthrough(src_blob, dst_blob)
        else:
            raise TypeError(f"Cannot handle copy operation for blob types '{type(src_blob)}' and '{type(dst_blob)}'")
        logger.debug(f"Copied {src_blob.url} to {dst_blob.url}")
    except Exception:
        logger.exception(f"copy failed: '{src_blob.url}' to '{dst_blob.url}'")
        try:
            dst_blob.delete()
        finally:
            pass
        raise

def blob_for_url(url: str) -> AnyBlob:
    if url.startswith(GSBlobStore.schema):
        parts = url[len(GSBlobStore.schema):].split("/", 1)
        if 2 != len(parts):
            raise ValueError(f"Incorrect GS url '{url}'")
        bucket, key = parts
        return GSBlob(bucket, key)
    elif url.startswith(URLBlobStore.schema):
        return URLBlob(url)
    else:
        return LocalBlob(os.path.sep, os.path.abspath(url))

class CopyClient:
    multipart_threshold = default_chunk_size

    def __init__(self,
                 concurrency: int=multiprocessing.cpu_count(),
                 raise_on_error: bool=False,
                 progress_indicator: Optional[str]=None):
        """If 'raise_on_error' is False, all copy operations will be attempted even if one or more operations error. If
        'raise_on_error' is True, the first error encountered will be raise and all scheduled operations will be
        canceled.
        """
        self._executor = ProcessPoolExecutor(max_workers=concurrency)
        self._queue = ConcurrentHeap(self._executor, concurrency)
        self.raise_on_error = raise_on_error
        self._old_indicator_type = Progress.indicator_type
        if progress_indicator:
            Progress.indicator_type = ProgressIndicatorType[progress_indicator]

    def copy(self, src: Union[str, AnyBlob], dst: Union[str, AnyBlob]):
        if isinstance(src, str):
            src = blob_for_url(src)
        if isinstance(dst, str):
            dst = blob_for_url(dst)
        priority = -src.size()
        self._queue.priority_put(priority, _do_copy, src, dst, self.multipart_threshold)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        try:
            for f in self._queue.iter_futures():
                if self.raise_on_error:
                    try:
                        f.result()
                    except Exception:
                        self._queue.abort()
                        raise
        finally:
            Progress.indicator_type = self._old_indicator_type
            self._executor.shutdown()

def copy(src: Union[str, AnyBlob], dst: Union[str, AnyBlob]):
    with CopyClient(progress_indicator="bar") as client:
        client.copy(src, dst)
