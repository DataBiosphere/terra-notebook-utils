import sys
import warnings
import contextlib

from io import TextIOWrapper, BytesIO


class SuppressWarningsMixin:
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
        # Suppress unclosed socket warnings
        warnings.simplefilter("ignore", ResourceWarning)

@contextlib.contextmanager
def encoded_bytes_stream():
    old_stdout = sys.stdout
    sys.stdout = TextIOWrapper(BytesIO(), sys.stdout.encoding)
    yield
    sys.stdout.close()
    sys.stdout = old_stdout
