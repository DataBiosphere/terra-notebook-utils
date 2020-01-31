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
    dst_bucket = dst_client.bucket(dst_bucket_name)

    dst_blob_names: list = list()
    for chunk in gs.iter_download(src_client, src_bucket_name, src_key, WORKSPACE_GOOGLE_PROJECT):
        part_number = len(dst_blob_names) + 1
        blob_name = f"{dst_key}.part{part_number}"
        dst_blob_names.append(blob_name)
        print("Uploading part", blob_name)
        dst_bucket.blob(blob_name).upload_from_file(io.BytesIO(chunk))
    gs.compose_parts(dst_bucket, dst_blob_names, dst_key)

def old_copy(drs_url: str, dst_key: str, dst_bucket: str=None):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    if dst_bucket is None:
        dst_bucket = WORKSPACE_BUCKET
    data_url, credentials_data = _resolve_drs_for_gs_storage(drs_url)
    src_bucket, src_key = _parse_gs_url(data_url)
    src_client = gs.get_client(credentials_data, project=credentials_data['project_id'])
    dst_client = gs.get_client()
    src_blob = src_client.bucket(src_bucket, user_project=WORKSPACE_GOOGLE_PROJECT).blob(src_key)
    dst_blob = dst_client.bucket(dst_bucket, user_project=WORKSPACE_GOOGLE_PROJECT).blob(dst_key)
    with NamedTemporaryFile("wb") as fh_write:
        src_blob.download_to_file(fh_write)
        fh_write.flush()
        with open(fh_write.name, "rb") as fh_read:
            dst_blob.upload_from_file(fh_read)

# genomes/NWD522743.b38.irc.v1.cram.crai
# download url
# https://storage.gsapis.com/download/storage/v1/b/topmed-irc-share/o/genomes%2FNWD522743.b38.irc.v1.cram.crai?alt=media&userProject=firecloud-cgl
# upload url
# https://storage.gsapis.com/upload/storage/v1/b/fc-d500be74-3672-458e-8e89-662a08922941/o?uploadType=resumable&userProject=firecloud-cgl
