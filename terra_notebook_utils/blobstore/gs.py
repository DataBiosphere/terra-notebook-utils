import io
import multiprocessing
from math import ceil
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Union, Generator

from getm import checksum, default_chunk_size
from getm.utils import indirect_open
import gs_chunked_io as gscio
from google.cloud.storage import Blob as GSNativeBlob, Bucket as GSNativeBucket
from google.api_core import exceptions as gcp_exceptions

from terra_notebook_utils import blobstore, IO_CONCURRENCY
from terra_notebook_utils import gs as gcp


class GSBlobStore(blobstore.BlobStore):
    schema = "gs://"

    def __init__(self,
                 bucket_name: str,
                 credentials: Optional[dict]=None,
                 billing_project: Optional[str]=None):
        self.bucket_name = bucket_name
        self.billing_project = billing_project
        self.credentials = credentials

    def blob(self, key: str) -> "GSBlob":
        return GSBlob(self.bucket_name, key, self.credentials, self.billing_project)

def _get_native_bucket(bucket: Union[str, GSNativeBucket],
                       credentials: Optional[dict]=None,
                       billing_project: Optional[str]=None) -> GSNativeBucket:
    if isinstance(bucket, str):
        kwargs = dict()
        if billing_project is not None:
            kwargs['user_project'] = billing_project
        bucket = gcp.get_client(credentials, billing_project).bucket(bucket, **kwargs)
    return bucket

def _get_native_blob(bucket: Union[str, GSNativeBucket],
                     key: str,
                     credentials: Optional[dict]=None,
                     billing_project: Optional[str]=None) -> GSNativeBlob:
    bucket = _get_native_bucket(bucket, credentials, billing_project)
    blob = bucket.get_blob(key)
    if blob is None:
        raise blobstore.BlobNotFoundError(f"Could not find gs://{bucket.name}/{key}")
    return blob

class GSBlob(blobstore.Blob):
    def __init__(self,
                 bucket_name: str,
                 key: str,
                 credentials: Optional[dict]=None,
                 billing_project: Optional[str]=None):
        self.bucket_name = bucket_name
        self.key = key
        self.billing_project = billing_project
        self.credentials = credentials

    # The next two methods customize pickling behavior. Some modules such as multiprocessing/ProcessPoolExecutor
    # require picklable objects. Let's make pickling snappy.
    # Pickle docs: https://docs.python.org/3/library/pickle.html#object.__getstate
    # multiprocessing docs: https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
    def __getstate__(self):
        return dict(bucket_name=self.bucket_name,
                    key=self.key,
                    credentials=self.credentials,
                    billing_project=self.billing_project)

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def _gs_bucket(self):
        if not getattr(self, "_bucket", None):
            self._bucket = _get_native_bucket(self.bucket_name, self.credentials, self.billing_project)
        return self._bucket

    def _get_native_blob(self) -> GSNativeBlob:
        return _get_native_blob(self._gs_bucket, self.key, self.credentials, self.billing_project)

    @property
    def url(self) -> str:  # type: ignore # TODO https://github.com/python/mypy/issues/4125
        return f"{GSBlobStore.schema}{self.bucket_name}/{self.key}"

    def get(self) -> bytes:
        return self._get_native_blob().download_as_bytes(checksum=None)

    def open(self, chunk_size: int=default_chunk_size):
        return self._get_native_blob().open(chunk_size=chunk_size, mode="rb")

    def put(self, data: bytes):
        blob = self._gs_bucket.blob(self.key)
        blob.upload_from_file(io.BytesIO(data))

    def delete(self):
        self._get_native_blob().delete()

    def copy_from(self, src_blob: "GSBlob", chunk_size: int=default_chunk_size):
        assert isinstance(src_blob, type(self))
        if self.url != src_blob.url:
            if not src_blob._gs_bucket.user_project:
                # TODO: always use rewrite when it support requester pays buckets
                dst_gs_blob = self._gs_bucket.blob(self.key)
                src_gs_blob = src_blob._gs_bucket.blob(src_blob.key)
                token: Optional[str] = None
                while True:
                    try:
                        resp = dst_gs_blob.rewrite(src_gs_blob, token)
                    except gcp_exceptions.NotFound:
                        raise blobstore.BlobNotFoundError(f"Could not find {src_blob.url}")
                    if resp[0] is None:
                        break
                    else:
                        token = resp[0]
            else:
                if src_blob.size() <= chunk_size:
                    self.put(src_blob.get())
                else:
                    with self.multipart_writer() as writer:
                        for part in src_blob.iter_content():
                            writer.put_part(part)

    def download(self, path: str):
        with indirect_open(path) as fh:
            cs = self.Hash()
            for part in self.iter_content():
                fh.write(part.data)
                cs.update(part.data)
            assert cs.matches(self.cloud_native_checksum())

    def exists(self) -> bool:
        blob = self._gs_bucket.blob(self.key)
        return blob.exists()

    def size(self) -> int:
        return self._get_native_blob().size

    def cloud_native_checksum(self) -> str:
        return self._get_native_blob().crc32c

    @property
    def Hash(self) -> checksum._Hasher:
        return checksum.GSCRC32C

    def iter_content(self, chunk_size: int=default_chunk_size) -> blobstore.PartIterator:
        return GSPartIterator(self.bucket_name,
                              self.key,
                              chunk_size,
                              credentials=self.credentials,
                              billing_project=self.billing_project)

    def multipart_writer(self) -> "blobstore.MultipartWriter":
        return GSMultipartWriter(self.bucket_name,
                                 self.key,
                                 credentials=self.credentials,
                                 billing_project=self.billing_project)

class GSPartIterator(blobstore.PartIterator):
    def __init__(self,
                 bucket_name: str,
                 key: str,
                 chunk_size: int,
                 credentials: Optional[dict]=None,
                 billing_project: Optional[str]=None):
        self._blob = _get_native_blob(bucket_name, key, credentials, billing_project)
        self.size = self._blob.size
        self.chunk_size = chunk_size
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1

    def __iter__(self) -> Generator[blobstore.Part, None, None]:
        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as e:
            q = gscio.async_collections.AsyncQueue(e)
            for chunk_number, data in enumerate(gscio.for_each_chunk(self._blob, self.chunk_size, q)):
                yield blobstore.Part(chunk_number, data)

class GSMultipartWriter(blobstore.MultipartWriter):
    def __init__(self,
                 bucket_name: str,
                 key: str,
                 credentials: Optional[dict]=None,
                 billing_project: Optional[str]=None):
        super().__init__()
        kwargs = dict()
        if billing_project is not None:
            kwargs['user_project'] = billing_project
        bucket = gcp.get_client(credentials, billing_project).bucket(bucket_name, **kwargs)
        self._executor = ThreadPoolExecutor(max_workers=IO_CONCURRENCY)
        async_set = gscio.async_collections.AsyncSet(self._executor, IO_CONCURRENCY)
        self._part_uploader = gscio.AsyncPartUploader(key, bucket, async_set)

    def put_part(self, part: blobstore.Part):
        self._part_uploader.put_part(part.number, part.data)

    def close(self):
        self._part_uploader.close()
        self._executor.shutdown()
