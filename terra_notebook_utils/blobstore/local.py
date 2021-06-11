import io
import os
import shutil
from math import ceil
from functools import wraps
from typing import Generator, Optional

from getm import default_chunk_size

from terra_notebook_utils import blobstore


def catch_blob_not_found(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except FileNotFoundError as ex:
            raise blobstore.BlobNotFoundError(f"Could not find {self.url}") from ex
    return wrapper

def catch_blob_not_found_generator(generator):
    @wraps(generator)
    def wrapper(self, *args, **kwargs):
        try:
            for item in generator(self, *args, **kwargs):
                yield item
        except FileNotFoundError as ex:
            raise blobstore.BlobNotFoundError(f"Could not find {self.url}") from ex
    return wrapper

class LocalBlobStore(blobstore.BlobStore):
    chunk_size = default_chunk_size

    def __init__(self, basepath: str):
        self.bucket_name = basepath

    def list(self, prefix: str="") -> Generator["LocalBlob", None, None]:
        root = os.path.join(self.bucket_name, prefix)
        if root.endswith(os.path.sep):
            root = root[:-len(os.path.sep)]
        for (dirpath, dirnames, filenames) in os.walk(root):
            for filename in filenames:
                relpath = os.path.relpath(os.path.join(dirpath, filename), self.bucket_name)
                yield LocalBlob(self.bucket_name, relpath)

    def blob(self, key: str) -> "LocalBlob":
        return LocalBlob(self.bucket_name, key)

class LocalBlob(blobstore.Blob):
    def __init__(self, basepath: str, relpath: str):
        assert basepath == os.path.abspath(basepath)
        self.bucket_name = basepath
        self.key = relpath
        self._path = os.path.join(basepath, relpath)
        self.makedirs = True  # create subdirectories if needed during write operations

    # The next two methods customize pickling behavior. Some modules such as multiprocessing/ProcessPoolExecutor
    # require picklable objects. Let's make pickling snappy.
    # Pickle docs: https://docs.python.org/3/library/pickle.html#object.__getstate
    # multiprocessing docs: https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
    def __getstate__(self):
        return dict(bucket_name=self.bucket_name, key=self.key, _path=self._path)

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def url(self) -> str:  # type: ignore # TODO https://github.com/python/mypy/issues/4125
        return self._path

    @catch_blob_not_found
    def get(self) -> bytes:
        with open(self._path, "rb") as fh:
            return fh.read()

    @catch_blob_not_found
    def open(self, chunk_size: Optional[int]=None) -> io.FileIO:
        return open(self._path, "rb")  # type: ignore  # this is technically BinaryIO but should be compatible

    def put(self, data: bytes):
        if self.makedirs:
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "wb") as fh:
            fh.write(data)

    @catch_blob_not_found
    def delete(self):
        os.remove(self._path)

    @catch_blob_not_found_generator
    def copy_from_iter(self, src_blob: "LocalBlob") -> Generator[int, None, None]:
        """
        Intra-cloud copy
        """
        assert isinstance(src_blob, type(self))
        if self.url != src_blob.url:
            shutil.copyfile(src_blob._path, self._path)
        yield self.size()

    def copy_from(self, src_blob: "LocalBlob"):
        for part in self.copy_from_iter(src_blob):
            pass

    def download_iter(self, target: str) -> Generator[int, None, None]:
        if not os.path.isfile(self._path):
            raise blobstore.BlobNotFoundError(f"Could not find {self.url}")
        if self._path != target:
            shutil.copyfile(self._path, target)
        yield self.size()

    def download(self, target: str):
        for _ in self.download_iter(target):
            pass

    def exists(self) -> bool:
        if os.path.isdir(self._path):
            raise ValueError(f"'{self._path}' exists but is not a file!")
        return os.path.isfile(self._path)

    @catch_blob_not_found
    def size(self) -> int:
        return os.path.getsize(self._path)

    def iter_content(self) -> blobstore.PartIterator:
        return LocalPartIterator(self._path)

    def part_writer(self) -> "LocalPartWriter":
        return LocalPartWriter(self._path, self.makedirs)

class LocalPartIterator(blobstore.PartIterator):
    def __init__(self, path: str):
        try:
            self.size = os.path.getsize(path)
        except FileNotFoundError:
            raise blobstore.BlobNotFoundError(f"Could not find {path}")
        self.chunk_size = LocalBlobStore.chunk_size
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1
        self.handle = open(path, "rb")

    def __len__(self):
        return self._number_of_parts

    def __iter__(self) -> Generator[bytes, None, None]:
        for _ in range(self._number_of_parts):
            yield self.handle.read(self.chunk_size)

    def close(self):
        if hasattr(self, "handle"):
            self.handle.close()

    def __del__(self):
        self.close()

class LocalPartWriter(blobstore.PartWriter):
    """This provides a consistent interface for blobs, but is mostly just a wrapper over a normal file handle."""
    def __init__(self, filepath: str, makedirs: bool):
        if makedirs:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self._fh = open(filepath, "wb")

    def put_part(self, part: bytes):
        self._fh.write(part)

    def close(self):
        self._fh.close()
