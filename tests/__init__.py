import contextlib
import sys

from io import TextIOWrapper, BytesIO


@contextlib.contextmanager
def encoded_bytes_stream():
    old_stdout = sys.stdout
    sys.stdout = TextIOWrapper(BytesIO(), sys.stdout.encoding)
    yield
    sys.stdout.close()
    sys.stdout = old_stdout
