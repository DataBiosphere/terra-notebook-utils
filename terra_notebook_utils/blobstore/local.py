import os
import shutil
from math import ceil
from functools import wraps
from typing import Generator

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

    def put(self, data: bytes):
        with open(self._path, "wb") as fh:
            fh.write(data)

    @catch_blob_not_found
    def delete(self):
        os.remove(self._path)

    @catch_blob_not_found
    def copy_from(self, src_blob: "LocalBlob"):
        """
        Intra-cloud copy
        """
        assert isinstance(src_blob, type(self))
        if self.url != src_blob.url:
            shutil.copyfile(src_blob._path, self._path)

    def download(self, target: str):
        if not os.path.isfile(self._path):
            raise blobstore.BlobNotFoundError(f"Could not find {self.url}")
        if self._path != target:
            shutil.copyfile(self._path, target)

    def exists(self) -> bool:
        if os.path.isdir(self._path):
            raise ValueError(f"'{self._path}' exists but is not a file!")
        return os.path.isfile(self._path)

    @catch_blob_not_found
    def size(self) -> int:
        return os.path.getsize(self._path)

    def iter_content(self) -> blobstore.PartIterator:
        return LocalPartIterator(self._path)

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

    def __iter__(self) -> Generator[blobstore.Part, None, None]:
        for part_number in range(self._number_of_parts):
            yield blobstore.Part(part_number, self.handle.read(self.chunk_size))

    def close(self):
        if hasattr(self, "handle"):
            self.handle.close()

    def __del__(self):
        self.close()
