import json
import requests

from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, GS_SCHEMA
from terra_notebook_utils import gs, tar_gz

import gs_chunked_io as gscio


def _parse_gs_url(gs_url):
    if gs_url.startswith(GS_SCHEMA):
        bucket_name, object_key = gs_url[len(GS_SCHEMA):].split("/", 1)
        return bucket_name, object_key
    else:
        raise RuntimeError(f'Invalid gs url schema.  {gs_url} does not start with {GS_SCHEMA}')

def fetch_drs_info(drs_url):
    access_token = gs.get_access_token()
    martha_url = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v2"
    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }
    resp = requests.post(martha_url, headers=headers, data=json.dumps(dict(url=drs_url)))
    if 200 == resp.status_code:
        resp_data = resp.json()
    else:
        print(resp.content)
        raise Exception(f"expected status 200, got {resp.status_code}")
    return resp_data

def resolve_drs_for_gs_storage(drs_url):
    drs_info = fetch_drs_info(drs_url)
    credentials_data = drs_info['googleServiceAccount']['data']
    for url_info in drs_info['dos']['data_object']['urls']:
        if url_info['url'].startswith(GS_SCHEMA):
            data_url = url_info['url']
            break
    else:
        raise Exception(f"Unable to resolve GS url for {drs_url}")
    bucket_name, key = _parse_gs_url(data_url)
    client = gs.get_client(credentials_data, project=credentials_data['project_id'])
    return client, bucket_name, key

def download(drs_url: str, filepath: str):
    client, bucket_name, key = resolve_drs_for_gs_storage(drs_url)
    blob = client.bucket(bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT).blob(key)
    with open(filepath, "wb") as fh:
        blob.download_to_file(fh)

def copy(drs_url: str, dst_key: str, dst_bucket_name: str=None, multipart_threshold=1024 * 1024 * 32):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    src_client, src_bucket_name, src_key = resolve_drs_for_gs_storage(drs_url)
    dst_client = gs.get_client()
    src_bucket = src_client.bucket(src_bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT)
    dst_bucket = dst_client.bucket(dst_bucket_name)
    gs.copy(src_bucket, dst_bucket, src_key, dst_key, multipart_threshold)

def extract_tar_gz(drs_url: str, dst_pfx: str=None, dst_bucket_name: str=None):
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    src_client, src_bucket_name, src_key = resolve_drs_for_gs_storage(drs_url)
    src_bucket = src_client.bucket(src_bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT)
    dst_bucket = gs.get_client().bucket(dst_bucket_name)
    with gscio.AsyncReader(src_bucket.get_blob(src_key), chunks_to_buffer=2) as fh:
        tar_gz.extract(fh, dst_bucket, root=dst_pfx)
