import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Union

from getm import default_chunk_size
from getm.concurrent.collections import ConcurrentHeap

from terra_notebook_utils.blobstore.gs import GSBlobStore, GSBlob
from terra_notebook_utils.blobstore.url import URLBlobStore, URLBlob
from terra_notebook_utils.blobstore.local import LocalBlobStore, LocalBlob
from terra_notebook_utils.blobstore.progress import Indicator
from terra_notebook_utils.blobstore import BlobstoreChecksumError
from terra_notebook_utils.logger import logger


AnyBlobStore = Union[GSBlobStore, URLBlobStore, LocalBlobStore]
AnyBlob = Union[GSBlob, URLBlob, LocalBlob]
CloudBlob = GSBlob

def _download(src_blob: AnyBlob, dst_blob: LocalBlob, indicator_type: Indicator):
    logger.debug(f"Starting download {src_blob.url} to {dst_blob.url}")
    dirname = os.path.dirname(dst_blob.url)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    # The download methods for each Blob is expected to compute checksums
    with Indicator.get(indicator_type, dst_blob.url, src_blob.size()) as indicator:
        for part_size in src_blob.download_iter(dst_blob.url):
            indicator.add(part_size)

def _copy_intra_cloud(src_blob: AnyBlob, dst_blob: AnyBlob, indicator_type: Indicator):
    # In general it is not necessary to compute checksums for intra cloud copies. The Storage services will do that for
    # us.
    # However, S3Etags depend on the part layout. Either ensure source and destination part layouts are the same, or
    # compute the destination S3Etag on the fly.
    logger.debug(f"Starting intra-cloud {src_blob.url} to {dst_blob.url}")
    assert isinstance(src_blob, type(dst_blob))
    with Indicator.get(indicator_type, dst_blob.url, src_blob.size()) as indicator:
        for part_size in dst_blob.copy_from_iter(src_blob):  # type: ignore
            indicator.add(part_size)
    if src_blob.cloud_native_checksum() != dst_blob.cloud_native_checksum():
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        raise BlobstoreChecksumError()

def _copy_oneshot_passthrough(src_blob: Union[URLBlob, CloudBlob], dst_blob: CloudBlob, indicator_type: Indicator):
    logger.debug(f"Starting oneshot passthrough {src_blob.url} to {dst_blob.url}")
    with Indicator.get(indicator_type, dst_blob.url, src_blob.size()) as indicator:
        data = src_blob.get()
        dst_blob.put(data)
        indicator.add(len(data))
    if dst_blob.md5 != src_blob.md5:
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        raise BlobstoreChecksumError()

def _copy_multipart_passthrough(src_blob: Union[URLBlob, CloudBlob], dst_blob: CloudBlob, indicator_type: Indicator):
    logger.debug(f"Starting multipart passthrough {src_blob.url} to {dst_blob.url}")
    with Indicator.get(indicator_type, dst_blob.url, src_blob.size()) as indicator:
        with dst_blob.part_writer() as writer:
            for part in src_blob.iter_content():
                writer.put_part(part)
                indicator.add(len(part))
    if dst_blob.md5 != src_blob.md5:
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        raise BlobstoreChecksumError()

def _do_copy(src_blob: AnyBlob, dst_blob: AnyBlob, multipart_threshold: int, indicator_type: Indicator):
    try:
        if isinstance(dst_blob, LocalBlob):
            _download(src_blob, dst_blob, indicator_type)
        elif isinstance(src_blob, type(dst_blob)):
            _copy_intra_cloud(src_blob, dst_blob, indicator_type)
        elif isinstance(dst_blob, CloudBlob):
            # The following assert prevents LocalBlob -> CloudBlob, i.e. upload. This should be removed in TNU is
            # expected to upload local files to cloud locations. Checksumming logic will also need updates.
            assert isinstance(src_blob, (URLBlob, CloudBlob))
            if src_blob.size() <= multipart_threshold:
                _copy_oneshot_passthrough(src_blob, dst_blob, indicator_type)
            else:
                _copy_multipart_passthrough(src_blob, dst_blob, indicator_type)
        else:
            raise TypeError(f"Cannot handle copy operation for blob types '{type(src_blob)}' and '{type(dst_blob)}'")
        logger.debug(f"Copied {src_blob.url} to {dst_blob.url}")
    except Exception:
        logger.exception(f"copy failed: '{src_blob.url}' to '{dst_blob.url}'")
        # FIXME: The finally block is useless, but what was the intent? Probably idempotent delete
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
                 indicator_type: Optional[Indicator]=None):
        """If 'raise_on_error' is False, all copy operations will be attempted even if one or more operations error. If
        'raise_on_error' is True, the first error encountered will be raise and all scheduled operations will be
        canceled.
        """
        self._executor = ProcessPoolExecutor(max_workers=concurrency)
        self._queue = ConcurrentHeap(self._executor, concurrency)
        self.raise_on_error = raise_on_error
        self.indicator_type = indicator_type or Indicator.log

    def copy(self, src: Union[str, AnyBlob], dst: Union[str, AnyBlob]):
        if isinstance(src, str):
            src = blob_for_url(src)
        if isinstance(dst, str):
            dst = blob_for_url(dst)
        priority = -src.size()
        self._queue.priority_put(priority, _do_copy, src, dst, self.multipart_threshold, self.indicator_type)

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
            self._executor.shutdown()

def copy(src: Union[str, AnyBlob], dst: Union[str, AnyBlob]):
    with CopyClient(indicator_type=Indicator.log) as client:
        client.copy(src, dst)
