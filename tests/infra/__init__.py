import io
import os
import logging
import requests
import warnings

from google.cloud.storage import Client


def get_env(name: str) -> str:
    val = os.environ.get(name)
    if val is None:
        raise RuntimeError(f"Please set the '{name}' environment variable to run tests.")
    return val

class SuppressWarningsMixin:
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
        # Suppress unclosed socket warnings
        warnings.simplefilter("ignore", ResourceWarning)

        # Suppress urllib3 warnings
        logging.getLogger(requests.packages.urllib3.__package__).setLevel(logging.ERROR)

def upload_data(uri: str, data: bytes):
    if uri.startswith("gs://"):
        bucket, key = uri[5:].split("/", 1)
        Client().bucket(bucket).blob(key).upload_from_file(io.BytesIO(data))
    else:
        with open(uri, "wb") as fh:
            fh.write(data)
