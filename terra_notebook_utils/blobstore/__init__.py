from collections import namedtuple
from typing import Any, Generator, IO, Optional

from getm import checksum


MiB = 1024 * 1024

class BlobStore:
    schema: str
    chunk_size: int

    def list(self, prefix: str=""):
        raise NotImplementedError()

    def blob(self, key: str):
        raise NotImplementedError()

class Blob:
    key: str
    url: str

    def get(self) -> bytes:
        raise NotImplementedError()

    def open(self, chunk_size: Optional[int]=None) -> IO:
        raise NotImplementedError()

    def put(self, data: bytes):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def copy_from(self, src_blob: Any) -> Generator[int, None, None]:
        """Intra-cloud copy. This yields the size of parts copied, and must be iterated"""
        raise NotImplementedError()

    def download(self, path: str) -> Generator[int, None, None]:
        """This yields the size of parts downloaded, and must be iterated"""
        raise NotImplementedError()

    def exists(self) -> bool:
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()

    def cloud_native_checksum(self) -> str:
        raise NotImplementedError()

    def hash_class(self) -> checksum._Hasher:
        return checksum.GSCRC32C

    def iter_content(self) -> "PartIterator":
        raise NotImplementedError()

    def multipart_writer(self) -> "MultipartWriter":
        raise NotImplementedError()

Part = namedtuple("Part", "number data")

class PartIterator:
    def __len__(self):
        raise NotImplementedError()

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
