import io
import os
import logging
from contextlib import closing

from google.cloud.storage import Client
from google.oauth2 import service_account
import google.auth
import gs_chunked_io as gscio

from terra_notebook_utils import WORKSPACE_BUCKET
from terra_notebook_utils.progress import ProgressBar

logging.getLogger("google.resumable_media.requests.download").setLevel(logging.WARNING)
logging.getLogger("gs_chunked_io.writer").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

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
    resp = requests.delete("http://broad-bond-prod.appspot.com/api/link/v1/fence/", headers=headers)
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

def oneshot_copy(src_bucket, dst_bucket, src_key, dst_key):
    """
    Download an object into memory from `src_bucket` and upload it to `dst_bucket`
    """
    fh = io.BytesIO()
    src_bucket.blob(src_key).download_to_file(fh)
    fh.seek(0)
    dst_bucket.blob(dst_key).upload_from_file(fh)

def multipart_copy(src_bucket, dst_bucket, src_key, dst_key):
    """
    Download an upjoct in chunks from `src_bucket` and upload each chunk to `dst_bucket` as separate objets.
    The objects are then composed into a single object, and the parts are deleted from `dst_bucket`.
    """
    src_blob = src_bucket.get_blob(src_key)
    print(f"Copying from {src_bucket.name}/{src_key}")
    print(f"Copying to {dst_bucket.name}/{dst_key}")
    with gscio.AsyncReader(src_blob) as reader:
        progress_bar = ProgressBar(1 + reader.number_of_chunks(),
                                   prefix="Copying:",
                                   size=src_blob.size // 1024 ** 2,
                                   units="MB")
        with closing(progress_bar):
            with gscio.AsyncWriter(dst_key, dst_bucket) as writer:
                for chunk in reader.for_each_chunk():
                    writer.write(chunk)
                    progress_bar.update()
            progress_bar.update()

def copy(src_bucket, dst_bucket, src_key, dst_key, multipart_threshold=1024 * 1024 * 32):
    src_blob = src_bucket.get_blob(src_key)
    if multipart_threshold >= src_blob.size:
        oneshot_copy(src_bucket, dst_bucket, src_key, dst_key)
    else:
        multipart_copy(src_bucket, dst_bucket, src_key, dst_key)
    src_blob.reload()
    dst_blob = dst_bucket.get_blob(dst_key)
    assert src_blob.crc32c == dst_blob.crc32c

def list_bucket(prefix="", bucket=WORKSPACE_BUCKET):
    for blob in get_client().bucket(bucket).list_blobs(prefix=prefix):
        yield blob.name
