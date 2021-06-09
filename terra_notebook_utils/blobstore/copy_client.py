import os
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import Union

from getm import default_chunk_size
from getm.concurrent.collections import ConcurrentHeap

from terra_notebook_utils.blobstore.gs import GSBlobStore, GSBlob
from terra_notebook_utils.blobstore.url import URLBlobStore, URLBlob
from terra_notebook_utils.blobstore.local import LocalBlobStore, LocalBlob
from terra_notebook_utils.blobstore import BlobstoreChecksumError


logger = logging.getLogger(__name__)

AnyBlobStore = Union[GSBlobStore, URLBlobStore, LocalBlobStore]
AnyBlob = Union[GSBlob, URLBlob, LocalBlob]
CloudBlob = GSBlob

def _finalize_copy(src_blob: AnyBlob, dst_blob: AnyBlob):
    logger.info(f"Copied {src_blob.url} to {dst_blob.url}")

def _download(src_blob: AnyBlob, dst_blob: LocalBlob):
    logger.debug(f"Starting download {src_blob.url} to {dst_blob.url}")
    dirname = os.path.dirname(dst_blob.url)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    # The download methods for each Blob is expected to compute checksums
    src_blob.download(dst_blob.url)
    _finalize_copy(src_blob, dst_blob)

def _copy_intra_cloud(src_blob: AnyBlob, dst_blob: AnyBlob):
    # In general it is not necesary to compute checksums for intra cloud copies. The Storage services will do that for
    # us.
    # However, S3Etags depend on the part layout. Either ensure source and destination part layouts are the same, or
    # compute the destination S3Etag on the fly.
    logger.debug(f"Starting intra-cloud {src_blob.url} to {dst_blob.url}")
    assert isinstance(src_blob, type(dst_blob))
    dst_blob.copy_from(src_blob)  # type: ignore
    if src_blob.cloud_native_checksum() != dst_blob.cloud_native_checksum():
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        dst_blob.delete()
        raise BlobstoreChecksumError()
    _finalize_copy(src_blob, dst_blob)

def _copy_oneshot_passthrough(src_blob: AnyBlob, dst_blob: CloudBlob):
    logger.debug(f"Starting oneshot passthrough {src_blob.url} to {dst_blob.url}")
    data = src_blob.get()
    dst_blob.put(data)
    if not dst_blob.Hasher(data).matches(dst_blob.cloud_native_checksum()):
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        dst_blob.delete()
        raise BlobstoreChecksumError()
    _finalize_copy(src_blob, dst_blob)

def _copy_multipart_passthrough(src_blob: AnyBlob, dst_blob: CloudBlob):
    logger.debug(f"Starting multipart passthrough {src_blob.url} to {dst_blob.url}")
    cs = dst_blob.Hasher()
    with dst_blob.multipart_writer() as writer:
        for part in src_blob.iter_content():
            writer.put_part(part)
            cs.update(part.data)
    if not cs.matches(dst_blob.cloud_native_checksum()):
        logger.error(f"Checksum failed for {src_blob.url} to {dst_blob.url}")
        dst_blob.delete()
        raise BlobstoreChecksumError()
    _finalize_copy(src_blob, dst_blob)

class CopyClient:
    multipart_threshold = default_chunk_size

    def __init__(self, concurrency: int=multiprocessing.cpu_count()):
        self._executor = ProcessPoolExecutor(max_workers=concurrency)
        self._queue = ConcurrentHeap(self._executor, concurrency)

    def copy(self, src_blob: AnyBlob, dst_blob: AnyBlob):
        size = src_blob.size()
        priority = -size
        if isinstance(dst_blob, LocalBlob):
            self._queue.priority_put(priority, _download, src_blob, dst_blob)
        elif isinstance(src_blob, type(dst_blob)):
            self._queue.priority_put(priority, _copy_intra_cloud, src_blob, dst_blob)
        else:
            if size <= self.multipart_threshold:
                self._queue.priority_put(priority, _copy_oneshot_passthrough, src_blob, dst_blob)
            else:
                self._queue.priority_put(priority, _copy_multipart_passthrough, src_blob, dst_blob)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        try:
            for _ in self._queue:
                pass
        finally:
            self._executor.shutdown()

def copy(src_blob: AnyBlob, dst_blob: AnyBlob):
    with CopyClient() as client:
        client.copy(src_blob, dst_blob)
