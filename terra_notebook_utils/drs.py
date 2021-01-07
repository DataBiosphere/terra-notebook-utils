"""
Utilities for working with DRS objects
"""
import os
import sys
import re
import json
import logging
import requests
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from collections import namedtuple
from typing import Tuple, Iterable, Optional, Callable, IO
from google.cloud.exceptions import NotFound, Forbidden

import gs_chunked_io as gscio
from gs_chunked_io import async_collections

from terra_notebook_utils import (WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME, MULTIPART_THRESHOLD,
                                  IO_CONCURRENCY, MARTHA_URL)
from terra_notebook_utils import gs, tar_gz, TERRA_DEPLOYMENT_ENV, _GS_SCHEMA


logger = logging.getLogger(__name__)

DRSInfo = namedtuple("DRSInfo", "credentials bucket_name key name size updated")

class GSBlobInaccessible(Exception):
    pass

class DRSResolutionError(Exception):
    pass

def _parse_gs_url(gs_url: str) -> Tuple[str, str]:
    if gs_url.startswith(_GS_SCHEMA):
        bucket_name, object_key = gs_url[len(_GS_SCHEMA):].split("/", 1)
        return bucket_name, object_key
    else:
        raise RuntimeError(f'Invalid gs url schema.  {gs_url} does not start with {_GS_SCHEMA}')

@lru_cache()
def enable_requester_pays(workspace_name: Optional[str]=WORKSPACE_NAME,
                          workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    assert workspace_name
    import urllib.parse
    encoded_workspace = urllib.parse.quote(workspace_name)
    rawls_url = (f"https://rawls.dsde-{TERRA_DEPLOYMENT_ENV}.broadinstitute.org/api/workspaces/"
                 f"{workspace_namespace}/{encoded_workspace}/enableRequesterPaysForLinkedServiceAccounts")
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

def fetch_drs_info(drs_url: str) -> dict:
    """
    Request DRS infromation from martha.
    """
    access_token = gs.get_access_token()

    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }

    logger.info(f"Resolving DRS uri '{drs_url}' through '{MARTHA_URL}'.")

    resp = requests.post(MARTHA_URL, headers=headers, data=json.dumps(dict(url=drs_url)))

    if 200 == resp.status_code:
        resp_data = resp.json()
    else:
        logger.warning(resp.content)
        response_json = resp.json()

        if 'response' in response_json:
            if 'text' in response_json['response']:
                error_details = f"Error: {response_json['response']['text']}"
            else:
                error_details = ""
        else:
            error_details = ""

        raise DRSResolutionError(f"Unexpected response while resolving DRS path. Expected status 200, got "
                                 f"{resp.status_code}. {error_details}")

    return resp_data

def info(drs_url: str) -> dict:
    """
    Return a curated subset of data from `fetch_drs_info`.
    """
    info = resolve_drs_info_for_gs_storage(drs_url)
    out = dict(name=info.name, size=info.size, updated=info.updated)
    out['url'] = f"gs://{info.bucket_name}/{info.key}"
    return out

def extract_credentials_from_drs_response(response: dict) -> Optional[dict]:
    if 'googleServiceAccount' not in response or response['googleServiceAccount'] is None:
        credentials_data = None
    else:
        credentials_data = response['googleServiceAccount']['data']

    return credentials_data

def convert_martha_v2_response_to_DRSInfo(drs_url: str, drs_response: dict) -> DRSInfo:
    """
    Convert response from martha_v2 to DRSInfo
    """
    if 'data_object' in drs_response['dos']:
        credentials_data = extract_credentials_from_drs_response(drs_response)
        data_object = drs_response['dos']['data_object']

        if 'urls' not in data_object:
            raise DRSResolutionError(f"No GS url found for DRS uri '{drs_url}'")
        else:
            data_url = None
            for url_info in data_object['urls']:
                if 'url' in url_info and url_info['url'].startswith(_GS_SCHEMA):
                    data_url = url_info['url']
                    break
            if data_url is None:
                raise DRSResolutionError(f"No GS url found for DRS uri '{drs_url}'")

        bucket_name, key = _parse_gs_url(data_url)
        return DRSInfo(credentials=credentials_data,
                       bucket_name=bucket_name,
                       key=key,
                       name=data_object.get('name'),
                       size=data_object.get('size'),
                       updated=data_object.get('updated'))
    else:
        raise DRSResolutionError(f"No metadata was returned for DRS uri '{drs_url}'")

def convert_martha_v3_response_to_DRSInfo(drs_url: str, drs_response: dict) -> DRSInfo:
    """
    Convert response from martha_v3 to DRSInfo
    """
    if 'gsUri' not in drs_response:
        raise DRSResolutionError(f"No GS url found for DRS uri '{drs_url}'")

    credentials_data = extract_credentials_from_drs_response(drs_response)

    return DRSInfo(credentials=credentials_data,
                   bucket_name=drs_response.get('bucket'),
                   key=drs_response.get('name'),
                   name=drs_response.get('fileName'),
                   size=drs_response.get('size'),
                   updated=drs_response.get('timeUpdated'))

def resolve_drs_info_for_gs_storage(drs_url: str) -> DRSInfo:
    """
    Attempt to resolve gs:// url and credentials for a DRS object.
    """
    assert drs_url.startswith("drs://")
    drs_response: dict = fetch_drs_info(drs_url)

    if 'dos' in drs_response:
        return convert_martha_v2_response_to_DRSInfo(drs_url, drs_response)
    else:
        return convert_martha_v3_response_to_DRSInfo(drs_url, drs_response)

def resolve_drs_for_gs_storage(drs_url: str) -> Tuple[gs.Client, DRSInfo]:
    """
    Attempt to resolve gs:// url and credentials for a DRS object. Instantiate and return the Google Storage client.
    """
    assert drs_url.startswith("drs://")

    try:
        info = resolve_drs_info_for_gs_storage(drs_url)
    except DRSResolutionError:
        raise
    except Exception:
        raise

    if info.credentials is not None:
        project_id = info.credentials['project_id']
    else:
        project_id = None

    client = gs.get_client(info.credentials, project=project_id)
    return client, info

def copy_to_local(drs_url: str,
                  filepath: str,
                  workspace_name: Optional[str]=WORKSPACE_NAME,
                  workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Copy a DRS object to the local filesystem.
    """
    assert drs_url.startswith("drs://")
    enable_requester_pays(workspace_name, workspace_namespace)
    client, info = resolve_drs_for_gs_storage(drs_url)
    blob = client.bucket(info.bucket_name, user_project=workspace_namespace).blob(info.key)
    if os.path.isdir(filepath):
        filename = info.name or info.key.rsplit("/", 1)[-1]
        filepath = os.path.join(os.path.abspath(filepath), filename)
    logger.info(f"Downloading {drs_url} to {filepath}")
    try:
        with open(filepath, "wb") as fh:
            blob.download_to_file(fh)
    except Exception:
        os.remove(filepath)
        raise

def head(drs_url: str,
         num_bytes: int = 1,
         buffer: int = MULTIPART_THRESHOLD,
         workspace_name: Optional[str] = WORKSPACE_NAME,
         workspace_namespace: Optional[str] = WORKSPACE_GOOGLE_PROJECT):
    """
    Head a DRS object by byte.

    :param drs_url: A drs:// schema URL.
    :param num_bytes: Number of bytes to print from the DRS object.
    :param workspace_name: The name of the terra workspace.
    :param workspace_namespace: The name of the terra workspace namespace.
    """
    assert drs_url.startswith("drs://"), f'Not a DRS schema: {drs_url}'
    enable_requester_pays(workspace_name, workspace_namespace)
    try:
        client, info = resolve_drs_for_gs_storage(drs_url)
        blob = client.bucket(info.bucket_name, user_project=workspace_namespace).blob(info.key)
        with gscio.Reader(blob, chunk_size=buffer) as handle:
            the_bytes = handle.read(num_bytes)

    except (DRSResolutionError, NotFound, Forbidden):
        raise GSBlobInaccessible(f'The DRS URL: {drs_url}\n'
                                 f'Could not be accessed because of:\n'
                                 f'{traceback.format_exc()}')
    return the_bytes

def copy_to_bucket(drs_url: str,
                   dst_key: str,
                   dst_bucket_name: str=None,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    assert drs_url.startswith("drs://")
    enable_requester_pays(workspace_name, workspace_namespace)
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    src_client, src_info = resolve_drs_for_gs_storage(drs_url)
    if not dst_key:
        dst_key = src_info.name or src_info.key.rsplit("/", 1)[-1]
    dst_client = gs.get_client()
    src_bucket = src_client.bucket(src_info.bucket_name, user_project=workspace_namespace)
    dst_bucket = dst_client.bucket(dst_bucket_name)
    logger.info(f"Beginning to copy from {src_bucket} to {dst_bucket}. This can take a while for large files...")
    gs.copy(src_bucket, dst_bucket, src_info.key, dst_key)

def copy(drs_url: str,
         dst: str,
         workspace_name: Optional[str]=WORKSPACE_NAME,
         workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Copy a DRS object to either the local filesystem, or to a Google Storage location if `dst` starts with "gs://".
    """
    assert drs_url.startswith("drs://")
    if dst.startswith("gs://"):
        bucket_name, key = _bucket_name_and_key(dst)
        copy_to_bucket(drs_url,
                       key,
                       bucket_name,
                       workspace_name=workspace_name,
                       workspace_namespace=workspace_namespace)
    else:
        copy_to_local(drs_url, dst, workspace_name, workspace_namespace)

def copy_batch(drs_urls: Iterable[str],
               dst: str,
               workspace_name: Optional[str]=WORKSPACE_NAME,
               workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    enable_requester_pays(workspace_name, workspace_namespace)
    with ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as oneshot_executor:
        oneshot_pool = async_collections.AsyncSet(oneshot_executor, concurrency=IO_CONCURRENCY)
        for drs_url in drs_urls:
            assert drs_url.startswith("drs://")
            src_client, src_info = resolve_drs_for_gs_storage(drs_url)
            src_bucket = src_client.bucket(src_info.bucket_name, user_project=workspace_namespace)
            src_blob = src_bucket.get_blob(src_info.key)
            basename = src_info.name or src_info.key.rsplit("/", 1)[-1]
            if dst.startswith("gs://"):
                if dst.endswith("/"):
                    raise ValueError("Bucket destination cannot end with '/'")
                dst_bucket_name, dst_pfx = _bucket_name_and_key(dst)
                dst_bucket = gs.get_client().bucket(dst_bucket_name)
                dst_key = f"{dst_pfx}/{basename}"
                if MULTIPART_THRESHOLD >= src_blob.size:
                    oneshot_pool.put(gs.oneshot_copy, src_bucket, dst_bucket, src_info.key, dst_key)
                else:
                    gs.multipart_copy(src_bucket, dst_bucket, src_info.key, dst_key)
            else:
                oneshot_pool.put(copy_to_local, drs_url, dst, workspace_name, workspace_namespace)
        for _ in oneshot_pool.consume():
            pass

def extract_tar_gz(drs_url: str,
                   dst_pfx: str=None,
                   dst_bucket_name: str=None,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Extract a `.tar.gz` archive resolved by a DRS url into a Google Storage bucket.
    """
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    enable_requester_pays(workspace_name, workspace_namespace)
    src_client, src_info = resolve_drs_for_gs_storage(drs_url)
    src_bucket = src_client.bucket(src_info.bucket_name, user_project=workspace_namespace)
    dst_bucket = gs.get_client().bucket(dst_bucket_name)
    with ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as e:
        async_queue = async_collections.AsyncQueue(e, IO_CONCURRENCY)
        with gscio.Reader(src_bucket.get_blob(src_info.key), async_queue=async_queue) as fh:
            tar_gz.extract(fh, dst_bucket, root=dst_pfx)

def _bucket_name_and_key(gs_url: str) -> Tuple[str, str]:
    assert gs_url.startswith("gs://")
    parts = gs_url[5:].split("/", 1)
    if 1 >= len(parts) or not parts[1]:
        bucket_name = parts[0]
        key = ""
    else:
        bucket_name, key = parts
    return bucket_name, key
