from collections import namedtuple
from typing import Any, Generator

from getm import checksum


MiB = 1024 * 1024

class BlobStore:
    schema = ""

    def list(self, prefix: str=""):
        raise NotImplementedError()

    def blob(self, key: str):
        raise NotImplementedError()

class Blob:
    key: str
    url: str

    def get(self) -> bytes:
        raise NotImplementedError()

    def put(self, data: bytes):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def copy_from(self, src_blob: Any):
        """Intra-cloud copy."""
        raise NotImplementedError()

    def download(self, path: str):
        raise NotImplementedError()

    def exists(self) -> bool:
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()

    def cloud_native_checksum(self) -> str:
        raise NotImplementedError()

    def hash_class(self) -> checksum._Hasher:
        return checksum.GSCRC32C

    # TODO: choose a better chunk_size
    def iter_content(self, chunk_size: int=1024 * 1024) -> "PartIterator":
        raise NotImplementedError()

    def multipart_writer(self) -> "MultipartWriter":
        raise NotImplementedError()

Part = namedtuple("Part", "number data")

class PartIterator:
    def __init__(self, bucket_name: str, key: str, chunk_size: int):
        self.size = 0
        self.chunk_size = 0
        self._number_of_parts = 0

    def __len__(self):
        return self._number_of_parts

    def __iter__(self) -> Generator[Part, None, None]:
        raise NotImplementedError()


class MultipartWriter:
    def put_part(self, part: Part):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class BlobStoreError(Exception):
    pass

class BlobNotFoundError(BlobStoreError):
    pass

class BlobStoreUnknownError(BlobStoreError):
    pass

class BlobstoreChecksumError(BlobStoreError):
    pass
