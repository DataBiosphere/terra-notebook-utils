import io
import os
import logging
import warnings
from math import ceil
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

from google.cloud.storage import Client
from google.oauth2 import service_account
import google.auth
import gs_chunked_io as gscio
from gs_chunked_io.async_collections import AsyncSet
import google.cloud.storage.bucket as GSBucket

from terra_notebook_utils import WORKSPACE_BUCKET, TERRA_DEPLOYMENT_ENV, MULTIPART_THRESHOLD, IO_CONCURRENCY
from terra_notebook_utils.progress import ProgressBar


logging.getLogger("google.resumable_media.requests.download").setLevel(logging.WARNING)
logging.getLogger("gs_chunked_io.writer").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


def get_access_token():
    """
    Retrieve the access token using the default GCP account
    returns the same result as `gcloud auth print-access-token`
    """
    if os.environ.get("TERRA_NOTEBOOK_GOOGLE_ACCESS_TOKEN"):
        token = os.environ['TERRA_NOTEBOOK_GOOGLE_ACCESS_TOKEN']
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        from oauth2client.service_account import ServiceAccountCredentials
        scopes = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
                                                                 scopes=scopes)
        token = creds.get_access_token().access_token
    else:
        import google.auth.transport.requests
        creds, projects = google.auth.default()
        creds.refresh(google.auth.transport.requests.Request())
        token = creds.token
    return token

def reset_bond_cache():
    import requests
    token = get_access_token()
    headers = {
        'authorization': f"Bearer {token}",
        'content-type': "application/json"
    }
    resp = requests.delete(f"http://broad-bond-{TERRA_DEPLOYMENT_ENV}.appspot.com/api/link/v1/fence/", headers=headers)
    print(resp.content)

def get_client(credentials_data: dict=None, project: str=None):
    kwargs = dict()
    if credentials_data is not None:
        creds = service_account.Credentials.from_service_account_info(credentials_data)
        kwargs['credentials'] = creds
    if project is not None:
        kwargs['project'] = project
    client = Client(**kwargs)
    if credentials_data is None:
        client._credentials.refresh(google.auth.transport.requests.Request())
    return client

def oneshot_copy(src_bucket: GSBucket, dst_bucket: GSBucket, src_key: str, dst_key: str):
    """
    Download an object into memory from `src_bucket` and upload it to `dst_bucket`
    """
    logger.info(f"Copying gs://{src_bucket.name}/{src_key} to gs://{dst_bucket.name}/{dst_key}")
    fh = io.BytesIO()
    src_bucket.blob(src_key).download_to_file(fh)
    fh.seek(0)
    dst_bucket.blob(dst_key).upload_from_file(fh)
    assert src_bucket.get_blob(src_key).crc32c == dst_bucket.get_blob(dst_key).crc32c

def multipart_copy(src_bucket: GSBucket, dst_bucket: GSBucket, src_key: str, dst_key: str):
    """
    Download/upload an object in parts from `src_bucket` to `dst_bucket`
    """
    src_blob = src_bucket.get_blob(src_key)
    print(f"Copying gs://{src_bucket.name}/{src_key} to gs://{dst_bucket.name}/{dst_key}")
    number_of_chunks = ceil(src_blob.size / gscio.reader.default_chunk_size)
    progress_bar = ProgressBar(1 + number_of_chunks,
                               prefix="Copying:",
                               size=src_blob.size // 1024 ** 2,
                               units="MB")
    with ThreadPoolExecutor(max_workers=2 * IO_CONCURRENCY) as e:
        async_uploads = AsyncSet(e, IO_CONCURRENCY)
        async_downloads = AsyncSet(e, IO_CONCURRENCY)
        with closing(progress_bar):
            with gscio.AsyncPartUploader(dst_key, dst_bucket, async_uploads) as writer:
                for chunk_number, chunk in gscio.for_each_chunk_async(src_blob, async_downloads):
                    writer.put_part(chunk_number, chunk)
                    progress_bar.update()
            progress_bar.update()
        assert src_bucket.get_blob(src_key).crc32c == dst_bucket.get_blob(dst_key).crc32c

def copy(src_bucket: GSBucket, dst_bucket: GSBucket, src_key: str, dst_key: str):
    src_blob = src_bucket.get_blob(src_key)
    if MULTIPART_THRESHOLD >= src_blob.size:
        oneshot_copy(src_bucket, dst_bucket, src_key, dst_key)
    else:
        multipart_copy(src_bucket, dst_bucket, src_key, dst_key)

def list_bucket(prefix="", bucket=WORKSPACE_BUCKET):
    for blob in get_client().bucket(bucket).list_blobs(prefix=prefix):
        yield blob.name
