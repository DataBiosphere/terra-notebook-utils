import io
import sys
import warnings
import contextlib

from google.cloud.storage import Client


class SuppressWarningsMixin:
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
        # Suppress unclosed socket warnings
        warnings.simplefilter("ignore", ResourceWarning)

@contextlib.contextmanager
def encoded_bytes_stream():
    old_stdout = sys.stdout
    sys.stdout = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
    yield
    sys.stdout.close()
    sys.stdout = old_stdout

def upload_data(uri: str, data: bytes):
    if uri.startswith("gs://"):
        bucket, key = uri[5:].split("/", 1)
        Client().bucket(bucket).blob(key).upload_from_file(io.BytesIO(data))
    else:
        with open(uri, "wb") as fh:
            fh.write(data)
