import io
import os
import json
import logging
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud.storage import Client
from google.oauth2 import service_account
import google.auth
import gs_chunked_io as gscio

from terra_notebook_utils.progress_bar import ProgressBar

logging.getLogger("google.resumable_media.requests.download").setLevel(logging.WARNING)
logging.getLogger("gs_chunked_io.writer").setLevel(logging.WARNING)

_default_chunk_size = 32 * 1024 * 1024
_gs_max_parts_per_compose = 32

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
    reader = gscio.Reader(src_blob)
    writer = gscio.Writer(dst_key, dst_bucket)
    progress_bar = ProgressBar(1 + reader.number_of_parts(),
                               prefix="Copying:",
                               size=src_blob.size // 1024 ** 2,
                               units="MB")

    def _transfer_chunk(part_number):
        data = reader.fetch_part(part_number)
        writer.put_part(part_number, data)
        progress_bar.update()

    with closing(progress_bar):
        with ThreadPoolExecutor(max_workers=3) as e:
            futures = [e.submit(_transfer_chunk, part_number) for part_number in range(reader.number_of_parts())]
            for f in as_completed(futures):
                pass
        writer.close()
        progress_bar.update()

def copy(src_bucket, dst_bucket, src_key, dst_key):
    src_blob = src_bucket.blob(src_key)
    src_blob.reload()
    if _default_chunk_size >= src_blob.size:
        oneshot_copy(src_bucket, dst_bucket, src_key, dst_key)
    else:
        multipart_copy(src_bucket, dst_bucket, src_key, dst_key)
    src_blob.reload()
    dst_blob = dst_bucket.get_blob(dst_key)
    assert src_blob.crc32c == dst_blob.crc32c
