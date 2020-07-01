"""
Utilities for working with DRS objects
"""
import os
import json
import typing
import requests
from collections import namedtuple

from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME
from terra_notebook_utils import gs, tar_gz, TERRA_DEPLOYMENT_ENV, _GS_SCHEMA
import functools
import logging

import gs_chunked_io as gscio


logger = logging.getLogger(__name__)

DRSInfo = namedtuple("DRSInfo", "credentials bucket_name key name size updated")

def _parse_gs_url(gs_url: str) -> typing.Tuple[str, str]:
    if gs_url.startswith(_GS_SCHEMA):
        bucket_name, object_key = gs_url[len(_GS_SCHEMA):].split("/", 1)
        return bucket_name, object_key
    else:
        raise RuntimeError(f'Invalid gs url schema.  {gs_url} does not start with {_GS_SCHEMA}')

@functools.lru_cache()
def enable_requester_pays(workspace_name: str=WORKSPACE_NAME,
                          google_billing_project: str=WORKSPACE_GOOGLE_PROJECT):
    import urllib.parse
    encoded_workspace = urllib.parse.quote(workspace_name)
    rawls_url = (f"https://rawls.dsde-{TERRA_DEPLOYMENT_ENV}.broadinstitute.org/api/workspaces/"
                 f"{google_billing_project}/{encoded_workspace}/enableRequesterPaysForLinkedServiceAccounts")
    logger.info("Enabling requester pays for your workspace. This will only take a few seconds...")
    access_token = gs.get_access_token()

    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }
    resp = requests.put(rawls_url, headers=headers)

    if resp.status_code != 204:
        logger.warning(f"Failed to init requester pays for workspace {WORKSPACE_GOOGLE_PROJECT}/{WORKSPACE_NAME}.")
        logger.warning("You will not be able to access drs urls that interact with requester pays buckets.")

def fetch_drs_info(
    drs_url: str,
    workspace_name: str=WORKSPACE_NAME,
    google_billing_project: str=WORKSPACE_GOOGLE_PROJECT
) -> dict:
    """
    Request DRS infromation from martha.
    """
    access_token = gs.get_access_token()

    martha_url = f"https://us-central1-broad-dsde-{TERRA_DEPLOYMENT_ENV}.cloudfunctions.net/martha_v2"
    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }

    resp = requests.post(martha_url, headers=headers, data=json.dumps(dict(url=drs_url)))

    if 200 == resp.status_code:
        resp_data = resp.json()
    else:
        logger.warning(resp.content)
        raise Exception(f"expected status 200, got {resp.status_code}")
    return resp_data

def resolve_drs_info_for_gs_storage(
    drs_url: str,
    workspace_name: str=WORKSPACE_NAME,
    google_billing_project: str=WORKSPACE_GOOGLE_PROJECT
) -> DRSInfo:
    """
    Attempt to resolve gs:// url and credentials for a DRS object.
    """
    drs_info = fetch_drs_info(drs_url, workspace_name, google_billing_project)
    credentials_data = drs_info['googleServiceAccount']['data']
    for url_info in drs_info['dos']['data_object']['urls']:
        if url_info['url'].startswith(_GS_SCHEMA):
            data_url = url_info['url']
            break
    else:
        raise Exception(f"Unable to resolve GS url for {drs_url}")

    bucket_name, key = _parse_gs_url(data_url)
    return DRSInfo(credentials=credentials_data,
                   bucket_name=bucket_name,
                   key=key,
                   name=drs_info['dos']['data_object'].get("name"),
                   size=drs_info['dos']['data_object'].get("size"),
                   updated=drs_info['dos']['data_object'].get("updated"))

def resolve_drs_for_gs_storage(
    drs_url: str,
    workspace_name: str=WORKSPACE_NAME,
    google_billing_project: str=WORKSPACE_GOOGLE_PROJECT
) -> typing.Tuple[gs.Client, DRSInfo]:
    """
    Attempt to resolve gs:// url and credentials for a DRS object. Instantiate and return the Google Storage client.
    """
    info = resolve_drs_info_for_gs_storage(drs_url,
                                           workspace_name,
                                           google_billing_project)
    client = gs.get_client(info.credentials, project=info.credentials['project_id'])
    return client, info

def copy_to_local(drs_url: str,
                  filepath: str,
                  workspace_name: str=WORKSPACE_NAME,
                  google_billing_project: str=WORKSPACE_GOOGLE_PROJECT):
    """
    Copy a DRS object to the local filesystem.
    """
    assert drs_url.startswith("drs://")
    client, info = resolve_drs_for_gs_storage(drs_url, workspace_name, google_billing_project)
    blob = client.bucket(info.bucket_name, user_project=google_billing_project).blob(info.key)
    if os.path.isdir(filepath):
        if info.name:
            filepath = os.path.join(os.path.abspath(filepath), info.name)
        else:
            raise ValueError("No DRS information for file name, file name must be provded.")
    with open(filepath, "wb") as fh:
        logger.info(f"Downloading {drs_url} to {filepath}")
        blob.download_to_file(fh)

def copy_to_bucket(drs_url: str,
                   dst_key: str,
                   dst_bucket_name: str=None,
                   multipart_threshold: int=1024 * 1024 * 32,
                   workspace_name: str=WORKSPACE_NAME,
                   google_billing_project: str=WORKSPACE_GOOGLE_PROJECT):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    assert drs_url.startswith("drs://")
    enable_requester_pays(workspace_name, google_billing_project)
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    src_client, src_info = resolve_drs_for_gs_storage(drs_url, workspace_name, google_billing_project)
    dst_client = gs.get_client()
    src_bucket = src_client.bucket(src_info.bucket_name, user_project=google_billing_project)
    dst_bucket = dst_client.bucket(dst_bucket_name)
    logger.info(f"Beginning to copy from {src_bucket} to {dst_bucket}. This can take a while for large files...")
    gs.copy(src_bucket, dst_bucket, src_info.key, dst_key, multipart_threshold)

def copy(drs_url: str,
         dst: str,
         workspace_name: str=WORKSPACE_NAME,
         google_billing_project: str=WORKSPACE_GOOGLE_PROJECT):
    """
    Copy a DRS object to either the local filesystem, or to a Google Storage location if `dst` starts with "gs://".
    """
    assert drs_url.startswith("drs://")
    enable_requester_pays(workspace_name, google_billing_project)
    if dst.startswith("gs://"):
        parts = dst[5:].split("/", 1)
        if 1 >= len(parts):
            raise ValueError("gs:// url should contain bucket name and key with '/' delimeter.")
        bucket_name, key = parts
        copy_to_bucket(drs_url,
                       key,
                       bucket_name,
                       workspace_name=workspace_name,
                       google_billing_project=google_billing_project)
    else:
        copy_to_local(drs_url, dst, workspace_name, google_billing_project)

def extract_tar_gz(drs_url: str,
                   dst_pfx: str=None,
                   dst_bucket_name: str=None,
                   workspace_name: str=WORKSPACE_NAME,
                   google_billing_project: str=WORKSPACE_GOOGLE_PROJECT):
    """
    Extract a `.tar.gz` archive resolved by a DRS url into a Google Storage bucket.
    """
    enable_requester_pays(workspace_name, google_billing_project)
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    assert dst_bucket_name
    src_client, src_info = resolve_drs_for_gs_storage(drs_url, workspace_name, google_billing_project)
    src_bucket = src_client.bucket(src_info.bucket_name, user_project=google_billing_project)
    dst_bucket = gs.get_client().bucket(dst_bucket_name)
    with gscio.AsyncReader(src_bucket.get_blob(src_info.key), chunks_to_buffer=2) as fh:
        tar_gz.extract(fh, dst_bucket, root=dst_pfx)
