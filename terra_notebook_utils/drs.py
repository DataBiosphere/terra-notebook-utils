import io
import json
import requests
from tempfile import NamedTemporaryFile

from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET
from terra_notebook_utils import gs

def _parse_gs_url(gs_url):
    bucket_name, object_key = gs_url[5:].split("/", 1)
    return bucket_name, object_key

def _resolve_drs_for_gs_storage(drs_url):
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
    # Get the gs url
    for url_info in resp_data['dos']['data_object']['urls']:
        if url_info['url'].startswith("gs://"):
            data_url = url_info['url']
            break
    else:
        raise Exception(f"Unable to resolve GS url for {drs_url}")
    # Get the service account credentials needed to access the gs url
    credentials_data = resp_data['googleServiceAccount']['data']
    return data_url, credentials_data

def download(drs_url: str, filepath: str):
    data_url, credentials_data = _resolve_drs_for_gs_storage(drs_url)
    src_bucket, object_key = _parse_gs_url(data_url)
    client = gs.get_client(credentials_data, project=credentials_data['project_id'])
    blob = client.bucket(src_bucket, user_project=WORKSPACE_GOOGLE_PROJECT).blob(object_key)
    with open(filepath, "wb") as fh:
        blob.download_to_file(fh)

def copy(drs_url: str, dst_key: str, dst_bucket_name: str=None):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    data_url, credentials_data = _resolve_drs_for_gs_storage(drs_url)
    src_bucket_name, src_key = _parse_gs_url(data_url)
    src_client = gs.get_client(credentials_data, project=credentials_data['project_id'])
    dst_client = gs.get_client()
    src_bucket = src_client.bucket(src_bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT)
    dst_bucket = dst_client.bucket(dst_bucket_name)
    gs.copy(src_bucket, dst_bucket, src_key, dst_key)
