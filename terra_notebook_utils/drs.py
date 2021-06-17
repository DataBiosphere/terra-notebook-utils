"""Utilities for working with DRS objects."""
import os
import logging
import traceback
from functools import lru_cache
from collections import namedtuple
from google.cloud.exceptions import NotFound, Forbidden
from typing import Dict, List, Tuple, Iterable, Optional, Union

from requests import Response

from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME, MARTHA_URL
from terra_notebook_utils import workspace, gs, tar_gz, TERRA_DEPLOYMENT_ENV, _GS_SCHEMA
from terra_notebook_utils.http import http
from terra_notebook_utils.blobstore.gs import GSBlob
from terra_notebook_utils.blobstore.local import LocalBlob
from terra_notebook_utils.blobstore import Blob, copy_client


logger = logging.getLogger(__name__)

DRSInfo = namedtuple("DRSInfo", "credentials access_url bucket_name key name size updated")

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
    resp = http.put(rawls_url, headers=headers)

    if resp.status_code != 204:
        logger.warning(f"Failed to init requester pays for workspace {WORKSPACE_GOOGLE_PROJECT}/{WORKSPACE_NAME}.")
        logger.warning("You will not be able to access drs urls that interact with requester pays buckets.")

def get_drs(drs_url: str) -> Response:
    """Request DRS infromation from martha."""
    access_token = gs.get_access_token()

    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }

    logger.debug(f"Resolving DRS uri '{drs_url}' through '{MARTHA_URL}'.")

    json_body = dict(url=drs_url)
    resp = http.post(MARTHA_URL, headers=headers, json=json_body)

    if 200 != resp.status_code:
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

    return resp

def info(drs_url: str) -> dict:
    """Return a curated subset of data from `get_drs`."""
    info = get_drs_info(drs_url)
    out = dict(name=info.name, size=info.size, updated=info.updated)
    out['url'] = f"gs://{info.bucket_name}/{info.key}"
    return out

def _get_drs_gs_creds(response: dict) -> Optional[dict]:
    service_account_info = response.get('googleServiceAccount')
    if service_account_info is not None:
        return service_account_info['data']
    else:
        return None

def _drs_info_from_martha_v2(drs_url: str, drs_data: dict) -> DRSInfo:
    """Convert response from martha_v2 to DRSInfo."""
    if 'data_object' in drs_data['dos']:
        data_object = drs_data['dos']['data_object']

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
        return DRSInfo(credentials=_get_drs_gs_creds(drs_data),
                       access_url=None,
                       bucket_name=bucket_name,
                       key=key,
                       name=data_object.get('name'),
                       size=data_object.get('size'),
                       updated=data_object.get('updated'))
    else:
        raise DRSResolutionError(f"No metadata was returned for DRS uri '{drs_url}'")

def _drs_info_from_martha_v3(drs_url: str, drs_data: dict) -> DRSInfo:
    """Convert response from martha_v3 to DRSInfo."""
    if 'gsUri' not in drs_data:
        raise DRSResolutionError(f"No GS url found for DRS uri '{drs_url}'")

    return DRSInfo(credentials=_get_drs_gs_creds(drs_data),
                   access_url=drs_data.get('accessUrl'),
                   bucket_name=drs_data.get('bucket'),
                   key=drs_data.get('name'),
                   name=drs_data.get('fileName'),
                   size=drs_data.get('size'),
                   updated=drs_data.get('timeUpdated'))

def get_drs_info(drs_url: str) -> DRSInfo:
    """Attempt to resolve gs:// url and credentials for a DRS object."""
    assert drs_url.startswith("drs://"), "Expected DRS URI of the form 'drs://...', got '{drs_url}'"
    drs_data = get_drs(drs_url).json()
    if 'dos' in drs_data:
        return _drs_info_from_martha_v2(drs_url, drs_data)
    else:
        return _drs_info_from_martha_v3(drs_url, drs_data)

def get_drs_blob(drs_url_or_info: Union[str, DRSInfo],
                 workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> GSBlob:
    if isinstance(drs_url_or_info, str):
        info = get_drs_info(drs_url_or_info)
    elif isinstance(drs_url_or_info, DRSInfo):
        info = drs_url_or_info
    else:
        raise TypeError()
    return GSBlob(info.bucket_name, info.key, info.credentials, workspace_namespace)

def blob_for_url(url: str, workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> Blob:
    if url.startswith("drs://"):
        return get_drs_blob(url, workspace_namespace)
    else:
        return copy_client.blob_for_url(url)

def _resolve_target(filepath: str, info: DRSInfo) -> str:
    if os.path.isdir(filepath):
        filename = info.name or info.key.rsplit("/", 1)[-1]
        filepath = os.path.join(os.path.abspath(filepath), filename)
    return filepath

def copy_to_local(drs_url: str,
                  filepath: str,
                  workspace_name: Optional[str]=WORKSPACE_NAME,
                  workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """Copy a DRS object to the local filesystem."""
    enable_requester_pays(workspace_name, workspace_namespace)
    info = get_drs_info(drs_url)
    copy_client.copy(get_drs_blob(info, workspace_namespace), _resolve_target(filepath, info))

def head(drs_url: str,
         num_bytes: int = 1,
         buffer: Optional[int]=None,
         workspace_name: Optional[str]=WORKSPACE_NAME,
         workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """Head a DRS object by byte."""
    enable_requester_pays(workspace_name, workspace_namespace)
    try:
        blob = get_drs_blob(drs_url, workspace_namespace)
        chunk_size = buffer or num_bytes
        with blob.open(chunk_size) as fh:
            the_bytes = fh.read(num_bytes)
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
    """Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.  If `dst_bucket` is None, copy into
    workspace bucket.
    """
    enable_requester_pays(workspace_name, workspace_namespace)
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    assert dst_bucket_name
    src_info = get_drs_info(drs_url)
    if not dst_key:
        dst_key = src_info.name or src_info.key.rsplit("/", 1)[-1]
    src_blob = get_drs_blob(src_info, workspace_namespace)
    dst_blob = GSBlob(dst_bucket_name, dst_key)
    copy_client.copy(src_blob, dst_blob)

def copy(drs_url: str,
         dst: str,
         workspace_name: Optional[str]=WORKSPACE_NAME,
         workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """Copy a DRS object to either the local filesystem, or to a Google Storage location if `dst` starts with
    "gs://".
    """
    if dst.startswith("gs://"):
        bucket_name, key = _bucket_name_and_key(dst)
        copy_to_bucket(drs_url,
                       key,
                       bucket_name,
                       workspace_name=workspace_name,
                       workspace_namespace=workspace_namespace)
    else:
        copy_to_local(drs_url, dst, workspace_name, workspace_namespace)

manifest_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "drs_uri": {"type": "string"},
            "dst": {"type": "string"},
        },
        "required": ["drs_uri", "dst"],
    },
}

def copy_batch(manifest: List[Dict[str, str]],
               workspace_name: Optional[str]=WORKSPACE_NAME,
               workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    from jsonschema import validate
    validate(instance=manifest, schema=manifest_schema)
    enable_requester_pays(workspace_name, workspace_namespace)
    with copy_client.CopyClient(progress_indicator="log") as cc:
        for item in manifest:
            src_info = get_drs_info(item['drs_uri'])
            src_blob = get_drs_blob(src_info, workspace_namespace)
            if item['dst'].startswith("gs://"):
                if item['dst'].endswith("/"):
                    raise ValueError("Bucket destination cannot end with '/'")
                basename = src_info.name or src_info.key.rsplit("/", 1)[-1]
                dst = f"{item['dst']}/{basename}"
            else:
                dst = _resolve_target(item['dst'], src_info)
            cc.copy(src_blob, dst)

def extract_tar_gz(drs_url: str,
                   dst: Optional[str]=None,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """Extract a `.tar.gz` archive resolved by a DRS url. 'dst' may be either a local filepath or a 'gs://' url.
    Default extraction is to the bucket for 'workspace'.
    """
    dst = dst or f"gs://{workspace.get_workspace_bucket(workspace_name)}"
    enable_requester_pays(workspace_name, workspace_namespace)
    blob = get_drs_blob(drs_url, workspace_namespace)
    with blob.open() as fh:
        tar_gz.extract(fh, dst)

def _bucket_name_and_key(gs_url: str) -> Tuple[str, str]:
    assert gs_url.startswith("gs://")
    parts = gs_url[5:].split("/", 1)
    if 1 >= len(parts) or not parts[1]:
        bucket_name = parts[0]
        key = ""
    else:
        bucket_name, key = parts
    return bucket_name, key
