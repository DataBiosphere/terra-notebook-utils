import json
import requests
from tempfile import NamedTemporaryFile

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob
from google.oauth2 import service_account

from . import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET

def _get_gcp_access_token():
    # This retrieves the access token using the default GCP account
    # returns the same result as `gcloud auth print-access-token`
    import google.auth.transport.requests
    creds, projects = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token

def _parse_gs_url(gs_url):
    bucket_name, object_key = gs_url[5:].split("/", 1)
    return bucket_name, object_key

def _get_gs_client(credentials_data: dict=None, project: str=None):
    kwargs = dict()
    if credentials_data is not None:
        creds = service_account.Credentials.from_service_account_info(credentials_data)
        kwargs['credentials'] = creds
    if project is not None:
        kwargs['project'] = project
    return Client(**kwargs)

def _resolve_drs_for_google_storage(drs_url) -> Blob:
    access_token = _get_gcp_access_token()
    martha_url = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v2"
    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }
    resp = requests.post(martha_url, headers=headers, data=json.dumps(dict(url=drs_url)))
    if 200 == resp.status_code:
        resp_data = resp.json()
    else:
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
    data_url, credentials_data = _resolve_drs_for_google_storage(drs_url)
    src_bucket, object_key = _parse_gs_url(data_url)
    src_client = _get_gs_client(credentials_data, project=credentials_data['project_id'])
    src_blob = src_client.bucket(src_bucket, user_project=WORKSPACE_GOOGLE_PROJECT).blob(object_key)
    with open(filepath, "wb") as fh:
        src_blob.download_to_file(fh)

def copy(drs_url: str, dst_key: str, dst_bucket: str=None):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    if dst_bucket is None:
        dst_bucket = WORKSPACE_BUCKET
    data_url, credentials_data = _resolve_drs_for_google_storage(drs_url)
    src_bucket, object_key = _parse_gs_url(data_url)
    src_client = _get_gs_client(credentials_data, project=credentials_data['project_id'])
    dst_client = _get_gs_client()
    src_blob = src_client.bucket(src_bucket, user_project=WORKSPACE_GOOGLE_PROJECT).blob(object_key)
    dst_blob = dst_client.bucket(dst_bucket, user_project=WORKSPACE_GOOGLE_PROJECT).blob(dst_key)
    with NamedTemporaryFile("wb") as fh_write:
        src_blob.download_to_file(fh_write)
        fh_write.flush()
        with open(fh_write.name, "rb") as fh_read:
            dst_blob.upload_from_file(fh_read)
    # TODO: Figure out how to use rewrite for cross-account copies
    # dst_blob.rewrite(src_blob)
