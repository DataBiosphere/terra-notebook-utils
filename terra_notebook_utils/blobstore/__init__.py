import io
from collections import namedtuple
from typing import Any, Generator, Optional

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

    def open(self, chunk_size: Optional[int]=None) -> io.FileIO:
        raise NotImplementedError()

    def put(self, data: bytes):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def copy_from_iter(self, src_blob: Any) -> Generator[int, None, None]:
        """Intra-cloud copy. This yields the size of parts copied, and must be iterated"""
        raise NotImplementedError()

    def copy_from(self, src_blob: Any) -> Generator[int, None, None]:
        """Intra-cloud copy."""
        raise NotImplementedError()

    def download_iter(self, path: str) -> Generator[int, None, None]:
        """This yields the size of parts downloaded, and must be iterated"""
        raise NotImplementedError()

    def download(self, path: str) -> Generator[int, None, None]:
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

    def part_writer(self) -> "PartWriter":
        raise NotImplementedError()

class PartIterator:
    def __len__(self):
        raise NotImplementedError()

    def __iter__(self) -> Generator[bytes, None, None]:
        raise NotImplementedError()

class PartWriter:
    def put_part(self, part: bytes):
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
