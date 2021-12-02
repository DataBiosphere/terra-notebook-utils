"""Utilities for working with DRS objects."""
import os
import requests

from functools import lru_cache
from collections import namedtuple
from typing import Dict, List, Tuple, Optional, Union, Iterable
from requests import Response

from terra_notebook_utils import WORKSPACE_BUCKET, WORKSPACE_NAME, MARTHA_URL, WORKSPACE_NAMESPACE, \
    WORKSPACE_GOOGLE_PROJECT
from terra_notebook_utils import workspace, gs, tar_gz, TERRA_DEPLOYMENT_ENV, _GS_SCHEMA
from terra_notebook_utils.utils import is_notebook
from terra_notebook_utils.http import http
from terra_notebook_utils.blobstore.gs import GSBlob
from terra_notebook_utils.blobstore.local import LocalBlob
from terra_notebook_utils.blobstore.url import URLBlob
from terra_notebook_utils.blobstore.progress import Indicator
from terra_notebook_utils.blobstore import Blob, copy_client, BlobNotFoundError
from terra_notebook_utils.logger import logger


DRSInfo = namedtuple("DRSInfo", "credentials access_url bucket_name key name size updated checksums")

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
                          workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE):
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
        logger.warning(f"Failed to init requester pays for workspace {workspace_namespace}/{workspace_name}: "
                       f"Expected '204', got '{resp.status_code}' for '{rawls_url}'. "
                       "You will not be able to access DRS URIs that interact with requester pays buckets.")

def get_drs(drs_url: str) -> Response:
    """Request DRS information from martha."""
    access_token = gs.get_access_token()

    headers = {
        'authorization': f"Bearer {access_token}",
        'content-type': "application/json"
    }

    logger.debug(f"Resolving DRS uri '{drs_url}' through '{MARTHA_URL}'.")

    json_body = dict(url=drs_url, fields=["fileName",
                                          "hashes",
                                          "size",
                                          "gsUri",
                                          "bucket",
                                          "name",
                                          "timeUpdated",
                                          "googleServiceAccount",
                                          "accessUrl"])
    resp = http.post(MARTHA_URL, headers=headers, json=json_body)

    if 200 != resp.status_code:
        logger.warning(resp.content)
        error_details = resp.json().get('response', {}).get('text', '')
        raise DRSResolutionError(f"Unexpected response while resolving DRS URI. Expected status 200, got "
                                 f"{resp.status_code}. {error_details}")

    return resp

def info(drs_url: str) -> dict:
    """Return a curated subset of data from `get_drs`."""
    info = get_drs_info(drs_url)
    return dict(name=info.name, size=info.size, updated=info.updated, checksums=info.checksums)

def access(drs_url: str,
           workspace_name: Optional[str]=WORKSPACE_NAME,
           workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE,
           billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> str:
    """Return a signed url for a drs:// URI, if available."""
    # We enable requester pays by specifying the workspace/namespace combo, not
    # with the billing project. Rawls then enables requester pays for the attached
    # project, but this won't work if a user specifies a project unattached to
    # the Terra workspace.
    enable_requester_pays(workspace_name, workspace_namespace)
    info = get_drs_info(drs_url)

    if info.access_url:
        return info.access_url

    url = gs.get_signed_url(bucket=info.bucket_name,
                            key=info.key,
                            sa_credentials=info.credentials)
    # attempt to get the first byte; we'll get an HTTPError error if we need requester pays
    # TODO: hopefully Martha returns this information eventually and we can avoid this check,
    #  but even in the meantime, there's probably a better way of doing this
    response = requests.get(url, headers={'Range': 'bytes=0-1'})
    if b'Bucket is a requester pays bucket' in response.content and response.status_code >= 400:
        return gs.get_signed_url(bucket=info.bucket_name,
                                 key=info.key,
                                 sa_credentials=info.credentials,
                                 requester_pays_user_project=billing_project)
    return url

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
                       checksums=None,
                       bucket_name=bucket_name,
                       key=key,
                       name=data_object.get('name'),
                       size=data_object.get('size'),
                       updated=data_object.get('updated'))
    else:
        raise DRSResolutionError(f"No metadata was returned for DRS uri '{drs_url}'")

def _drs_info_from_martha_v3(drs_url: str, drs_data: dict) -> DRSInfo:
    """Convert response from martha_v3 to DRSInfo."""
    access_url: Optional[str] = None
    if drs_data.get('accessUrl') is not None:
        access_url = drs_data['accessUrl'].get('url')

    if 'gsUri' not in drs_data and not access_url:
        raise DRSResolutionError(f"Neither GS URL or access URL found in DRS response for '{drs_url}'")

    return DRSInfo(credentials=_get_drs_gs_creds(drs_data),
                   access_url=access_url,
                   checksums=drs_data.get('hashes'),
                   bucket_name=drs_data.get('bucket'),
                   key=drs_data.get('name'),
                   name=drs_data.get('fileName'),
                   size=drs_data.get('size'),
                   updated=drs_data.get('timeUpdated'))

def get_drs_info(drs_url: str) -> DRSInfo:
    """Attempt to resolve gs:// url and credentials for a DRS object."""
    assert drs_url.startswith("drs://"), f"Expected DRS URI of the form 'drs://...', got '{drs_url}'"
    drs_data = get_drs(drs_url).json()
    if 'dos' in drs_data:
        return _drs_info_from_martha_v2(drs_url, drs_data)
    else:
        return _drs_info_from_martha_v3(drs_url, drs_data)

def get_drs_blob(drs_url_or_info: Union[str, DRSInfo],
                 billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> Union[GSBlob, URLBlob]:
    if isinstance(drs_url_or_info, str):
        info = get_drs_info(drs_url_or_info)
    elif isinstance(drs_url_or_info, DRSInfo):
        info = drs_url_or_info
    else:
        raise TypeError(f'Unexpected DRS input type ({type(drs_url_or_info)}): {drs_url_or_info}')
    blob: Union[URLBlob, GSBlob]
    if info.access_url is not None:
        blob = URLBlob(info.access_url, info.checksums.get('md5'))
    else:
        if not (info.credentials or info.bucket_name or info.key):
            raise ValueError(f'DRS information is missing.  Check:\n{info}')
        blob = GSBlob(info.bucket_name, info.key, info.credentials, billing_project)
    return blob

def blob_for_url(url: str, billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> Blob:
    if url.startswith("drs://"):
        return get_drs_blob(url, billing_project)
    else:
        return copy_client.blob_for_url(url)

def head(drs_url: str,
         num_bytes: int = 1,
         workspace_name: Optional[str]=WORKSPACE_NAME,
         workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE,
         billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """Head a DRS object by byte."""
    enable_requester_pays(workspace_name, workspace_namespace)
    try:
        blob = get_drs_blob(drs_url, billing_project)
        with blob.open(chunk_size=num_bytes) as fh:
            the_bytes = fh.read(num_bytes)
    except (DRSResolutionError, BlobNotFoundError) as e:
        raise BlobNotFoundError(f"The DRS URI: '{drs_url}' could not be accessed.") from e
    return the_bytes

def _resolve_bucket_target(url: str, info: DRSInfo) -> Tuple[str, str]:
    bucket_name, pfx = _bucket_name_and_key(url)
    if not pfx or pfx.endswith("/"):
        if pfx.endswith("/"):
            pfx = pfx[:-1]
        basename = info.name or info.key.rsplit("/", 1)[-1]
        key = f"{pfx}/{basename}" if pfx else basename
    else:
        key = pfx
    return bucket_name, key

def _resolve_local_target(filepath: str, info: DRSInfo) -> str:
    # Checking for a file also prevents a batch_copy from overwriting
    # the same file over and over again.
    if os.path.isfile(filepath):
        raise FileExistsError(f'Cannot copy {info.name} to {filepath} because file already exists')
    if filepath.endswith(os.path.sep) or os.path.isdir(filepath):
        filename = info.name or info.key.rsplit("/", 1)[-1]
        filepath = os.path.join(os.path.abspath(filepath), filename)
    return filepath

def _do_copy_drs(drs_uri: str,
                 dst: str,
                 multipart_threshold: int,
                 indicator_type: Indicator):
    dst_blob: Union[GSBlob, URLBlob, LocalBlob]
    src_info = get_drs_info(drs_uri)
    src_blob = get_drs_blob(src_info)
    if dst.startswith("gs://"):
        bucket_name, key = _resolve_bucket_target(dst, src_info)
        dst_blob = GSBlob(bucket_name, key)
    else:
        info = get_drs_info(drs_uri)
        dst_blob = copy_client.blob_for_url(_resolve_local_target(dst, info))
    copy_client._do_copy(src_blob, dst_blob, multipart_threshold, indicator_type)

class DRSCopyClient(copy_client.CopyClient):
    workspace: Optional[str] = None
    workspace_namespace: Optional[str] = None

    def copy(self, drs_uri: str, dst: str):  # type: ignore
        self._queue.put(_do_copy_drs,
                        drs_uri,
                        dst,
                        self.multipart_threshold,
                        self.indicator_type)

def copy(drs_uri: str,
         dst: str,
         indicator_type: Indicator=Indicator.notebook_bar if is_notebook() else Indicator.bar,
         workspace_name: Optional[str]=WORKSPACE_NAME,
         workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE):
    """Copy a DRS object to either the local filesystem, or to a Google Storage location if `dst` starts with
    "gs://".
    """
    enable_requester_pays(workspace_name, workspace_namespace)
    with DRSCopyClient(raise_on_error=True, indicator_type=indicator_type) as cc:
        cc.workspace = workspace_name
        cc.workspace_namespace = workspace_namespace
        cc.copy(drs_uri, dst or ".")

def copy_to_bucket(drs_uri: str,
                   dst_key: str="",
                   dst_bucket_name: Optional[str]=None,
                   indicator_type: Indicator=Indicator.notebook_bar if is_notebook() else Indicator.bar,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE):
    """Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.  If `dst_bucket` is None, copy into
    workspace bucket.
    """
    dst_bucket_name = dst_bucket_name or WORKSPACE_BUCKET
    dst_url = f"gs://{dst_bucket_name}"
    if dst_key:
        dst_url += f"/{dst_key}"
    copy(drs_uri, dst_url, indicator_type, workspace_name, workspace_namespace)

def copy_batch(drs_urls: Optional[Iterable[str]] = None,
               dst_pfx: Optional[str] = None,
               workspace_name: Optional[str] = WORKSPACE_NAME,
               workspace_namespace: Optional[str] = WORKSPACE_NAMESPACE,
               indicator_type: Indicator = Indicator.notebook_bar if is_notebook() else Indicator.log,
               manifest: Optional[List[Dict[str, str]]] = None):
    if (manifest is None) == (drs_urls is None):
        raise ValueError("Specify either 'manifest' or 'drs_urls' and 'dst_pfx'")
    elif manifest is not None:
        if dst_pfx is not None:
            raise ValueError('dst_pfx not compatible with manifest')
        copy_batch_manifest(manifest, indicator_type, workspace_name, workspace_namespace)
    elif drs_urls is not None:
        if dst_pfx is None:
            raise ValueError('dst_pfx required with drs_urls')
        copy_batch_urls(drs_urls, dst_pfx, indicator_type, workspace_name, workspace_namespace)
    else:
        assert False

def copy_batch_urls(drs_urls: Iterable[str],
                    dst_pfx: str,
                    indicator_type: Indicator = Indicator.notebook_bar if is_notebook() else Indicator.log,
                    workspace_name: Optional[str] = WORKSPACE_NAME,
                    workspace_namespace: Optional[str] = WORKSPACE_NAMESPACE):
    enable_requester_pays(workspace_name, workspace_namespace)
    with DRSCopyClient(indicator_type=indicator_type) as cc:
        cc.workspace = workspace_name
        cc.workspace_namespace = workspace_namespace
        for drs_url in drs_urls:
            cc.copy(drs_url, dst_pfx)

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

def copy_batch_manifest(manifest: List[Dict[str, str]],
                        indicator_type: Indicator=Indicator.notebook_bar if is_notebook() else Indicator.log,
                        workspace_name: Optional[str]=WORKSPACE_NAME,
                        workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE):
    from jsonschema import validate
    validate(instance=manifest, schema=manifest_schema)
    enable_requester_pays(workspace_name, workspace_namespace)
    with DRSCopyClient(indicator_type=indicator_type) as cc:
        cc.workspace = workspace_name
        cc.workspace_namespace = workspace_namespace
        for item in manifest:
            cc.copy(item['drs_uri'], item['dst'])

def extract_tar_gz(drs_url: str,
                   dst: Optional[str]=None,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   workspace_namespace: Optional[str]=WORKSPACE_NAMESPACE,
                   billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """Extract a `.tar.gz` archive resolved by a DRS url. 'dst' may be either a local filepath or a 'gs://' url.
    Default extraction is to the bucket for 'workspace'.
    """
    dst = dst or f"gs://{workspace.get_workspace_bucket(workspace_name)}"
    enable_requester_pays(workspace_name, workspace_namespace)
    blob = get_drs_blob(drs_url, billing_project)
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
