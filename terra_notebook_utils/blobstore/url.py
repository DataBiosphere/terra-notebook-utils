import io
from functools import wraps
from typing import Generator, Optional, Tuple

from requests.exceptions import HTTPError, ConnectionError
from getm.reader import URLRawReader, URLReaderKeepAlive
from getm.http import http
from getm import checksum, default_chunk_size_keep_alive
from getm.utils import indirect_open

from terra_notebook_utils import blobstore


def catch_blob_not_found(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except (HTTPError, ConnectionError) as ex:
            raise blobstore.BlobNotFoundError(f"Could not find {self.url}") from ex
    return wrapper

def catch_blob_not_found_generator(generator):
    @wraps(generator)
    def wrapper(self, *args, **kwargs):
        try:
            for item in generator(self, *args, **kwargs):
                yield item
        except (HTTPError, ConnectionError) as ex:
            raise blobstore.BlobNotFoundError(f"Could not find {self.url}") from ex
    return wrapper

class URLBlobStore(blobstore.BlobStore):
    schema: Tuple[str, str] = ("http://", "https://")  # type: ignore
    chunk_size = default_chunk_size_keep_alive

    def blob(self, url: str) -> "URLBlob":
        return URLBlob(url)

class URLBlob(blobstore.Blob):
    def __init__(self, url: str, md5: str=""):
        assert url.startswith(URLBlobStore.schema)
        self.url = self.key = url
        self.md5 = md5

    # The next two methods customize pickling behavior. Some modules such as multiprocessing/ProcessPoolExecutor
    # require picklable objects. Let's make pickling snappy.
    # Pickle docs: https://docs.python.org/3/library/pickle.html#object.__getstate
    # multiprocessing docs: https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
    def __getstate__(self):
        return dict(key=self.key, url=self.url)

    def __setstate__(self, state):
        self.__dict__.update(state)

    @catch_blob_not_found
    def get(self) -> bytes:
        with URLRawReader(self.url) as fh:
            return fh.read()

    @catch_blob_not_found
    def open(self, chunk_size: Optional[int]=None) -> io.FileIO:
        return URLRawReader(self.url)

    @catch_blob_not_found_generator
    def download_iter(self, path: str) -> Generator[int, None, None]:
        if self.md5:
            cs = checksum.GETMChecksum(self.md5, "md5")
        else:
            checksums = http.checksums(self.url)
            if 'gs_crc32c' in checksums:
                cs = checksum.GETMChecksum(checksums['gs_crc32c'], "gs_crc32c")
            elif 'gs_md5' in checksum:
                cs = checksum.GETMChecksum(checksums['gs_md5'], "md5")
            elif 'etag' in checksum:
                cs = checksum.GETMChecksum(checksums['gs_md5'], "md5")
            else:
                # TODO: warn user that data integrity checking is disabled
                cs = checksum.GETMChecksum("", checksum.Algorithms.null)

        with indirect_open(path) as fh:
            if http.size(self.url) < URLBlobStore.chunk_size:
                data = self.get()
                cs.update(data)
                fh.write(data)
                yield self.size()
            else:
                for part in self.iter_content():
                    cs.update(part)
                    fh.write(part)
                    yield len(part)
            assert cs.matches(), "Checksum failed!"

    def download(self, path: str):
        for _ in self.download_iter(path):
            pass

    @catch_blob_not_found
    def size(self) -> int:
        return http.size(self.url)

    def iter_content(self) -> blobstore.PartIterator:
        self.size()  # raise BlobNotFoundError
        return URLPartIterator(self.url, URLBlobStore.chunk_size)

class URLPartIterator(blobstore.PartIterator):
    def __init__(self, url: str, chunk_size: int):
        self.url = url
        self.chunk_size = chunk_size

    def __iter__(self):
        if 0 == http.size(self.url):
            yield b""
        else:
            for data in URLReaderKeepAlive.iter_content(self.url, self.chunk_size):
                yield data
