"""
Utilities for working with DRS objects
"""
import os
import sys
import io
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

from terra_notebook_utils import (WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME, MULTIPART_THRESHOLD,
                                  IO_CONCURRENCY)
from terra_notebook_utils import gs, tar_gz, TERRA_DEPLOYMENT_ENV, _GS_SCHEMA

import gs_chunked_io as gscio
from gs_chunked_io import async_collections


logger = logging.getLogger(__name__)

DRSInfo = namedtuple("DRSInfo", "credentials bucket_name key name size updated")

class GSBlobInaccessible(Exception):
    pass

def _parse_gs_url(gs_url: str) -> Tuple[str, str]:
    if gs_url.startswith(_GS_SCHEMA):
        bucket_name, object_key = gs_url[len(_GS_SCHEMA):].split("/", 1)
        return bucket_name, object_key
    else:
        raise RuntimeError(f'Invalid gs url schema.  {gs_url} does not start with {_GS_SCHEMA}')

@lru_cache()
def enable_requester_pays(workspace_name: Optional[str]=WORKSPACE_NAME,
                          google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    assert workspace_name
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

def fetch_drs_info(drs_url: str) -> dict:
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
        # TODO: Terra returns every error as a 502; return the real error code here: (resp.json()['status'])
        raise RuntimeError(f"expected status 200, got {resp.status_code}")
    return resp_data

def resolve_drs_info_for_gs_storage(drs_url: str) -> DRSInfo:
    """
    Attempt to resolve gs:// url and credentials for a DRS object.
    """
    assert drs_url.startswith("drs://")
    drs_info = fetch_drs_info(drs_url)
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

def resolve_drs_for_gs_storage(drs_url: str) -> Tuple[gs.Client, DRSInfo]:
    """
    Attempt to resolve gs:// url and credentials for a DRS object. Instantiate and return the Google Storage client.
    """
    assert drs_url.startswith("drs://")
    info = resolve_drs_info_for_gs_storage(drs_url)
    client = gs.get_client(info.credentials, project=info.credentials['project_id'])
    return client, info

def copy_to_local(drs_url: str,
                  filepath: str,
                  workspace_name: Optional[str]=WORKSPACE_NAME,
                  google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Copy a DRS object to the local filesystem.
    """
    assert drs_url.startswith("drs://")
    enable_requester_pays(workspace_name, google_billing_project)
    client, info = resolve_drs_for_gs_storage(drs_url)
    blob = client.bucket(info.bucket_name, user_project=google_billing_project).blob(info.key)
    if os.path.isdir(filepath):
        filename = info.name or _url_basename(drs_url)
        filepath = os.path.join(os.path.abspath(filepath), filename)
    with open(filepath, "wb") as fh:
        logger.info(f"Downloading {drs_url} to {filepath}")
        blob.download_to_file(fh)

def head(drs_url: str,
         num_bytes: int = 1,
         buffer: int = MULTIPART_THRESHOLD,
         workspace_name: Optional[str] = WORKSPACE_NAME,
         google_billing_project: Optional[str] = WORKSPACE_GOOGLE_PROJECT):
    """
    Head a DRS object by byte.

    :param drs_url: A drs:// schema URL.
    :param num_bytes: Number of bytes to print from the DRS object.
    :param workspace_name: The name of the terra workspace.
    :param google_billing_project: The name of the terra google billing project.
    """
    assert drs_url.startswith("drs://"), f'Not a DRS schema: {drs_url}'
    enable_requester_pays(workspace_name, google_billing_project)
    try:
        client, info = resolve_drs_for_gs_storage(drs_url)
    except Exception as e:
        # terra always returns a 502 for everything
        if 'got 502' in e.__repr__():
            raise GSBlobInaccessible(f'The DRS URL: {drs_url}\n'
                                     f'Could not be accessed because of:\n'
                                     f'{traceback.format_exc()}')
    blob = client.bucket(info.bucket_name, user_project=google_billing_project).blob(info.key)
    try:
        # sys.stdout.buffer is used outside of a python notebook
        # INSIDE of a python notebook, this is a ipykernel.iostream.OutStream object:
        # https://github.com/ipython/ipykernel/blob/master/ipykernel/iostream.py#L265
        # this seems to be able to handle bytes as well as unicode
        stdout_buffer = getattr(sys.stdout, 'buffer', None)

        if not stdout_buffer:
            # We're in a notebook, where instead of sending messages to sys.stdout.buffer,
            # we need to send serialized messages to a Session object.
            # Since we want to print bytes instead of the object's default unicode,
            # we rework the notebook's internal method.
            from ipykernel.iostream import OutStream
            from functools import partial
            class BytesOutStream(OutStream):
                def write(self, string):
                    is_child = (not self._is_master_process())
                    # only touch the buffer in the IO thread to avoid races
                    self.pub_thread.schedule(lambda: self._buffer.write(string))
                    if is_child:
                        # mp.Pool cannot be trusted to flush promptly (or ever),
                        # and this helps.
                        if self._subprocess_flush_pending:
                            return
                        self._subprocess_flush_pending = True
                        # We can not rely on self._io_loop.call_later from a subprocess
                        self.pub_thread.schedule(self._flush)
                    else:
                        self._schedule_flush()

                def _new_buffer(self):
                    self._buffer = io.BytesIO()

                def read(self):
                    raise NotImplementedError("I... can't... READ... T____T ")

            setattr(sys.stdout.session, 'packer', 'pickle')
            setattr(sys.stdout, 'write', partial(BytesOutStream.write, sys.stdout))
            setattr(sys.stdout, '_new_buffer', partial(BytesOutStream._new_buffer, sys.stdout))
            sys.stdout._new_buffer()
            stdout_buffer = sys.stdout

        with gscio.Reader(blob, chunk_size=buffer) as handle:
            stdout_buffer.write(handle.read(num_bytes))
    except (NotFound, Forbidden):
        raise GSBlobInaccessible(f'The DRS URL: {drs_url}\n'
                                 f'Could not be accessed because of:\n'
                                 f'{traceback.format_exc()}')

def copy_to_bucket(drs_url: str,
                   dst_key: str,
                   dst_bucket_name: str=None,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Resolve `drs_url` and copy into user-specified bucket `dst_bucket`.
    If `dst_bucket` is None, copy into workspace bucket.
    """
    assert drs_url.startswith("drs://")
    enable_requester_pays(workspace_name, google_billing_project)
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    src_client, src_info = resolve_drs_for_gs_storage(drs_url)
    dst_client = gs.get_client()
    src_bucket = src_client.bucket(src_info.bucket_name, user_project=google_billing_project)
    dst_bucket = dst_client.bucket(dst_bucket_name)
    logger.info(f"Beginning to copy from {src_bucket} to {dst_bucket}. This can take a while for large files...")
    gs.copy(src_bucket, dst_bucket, src_info.key, dst_key)

def copy(drs_url: str,
         dst: str,
         workspace_name: Optional[str]=WORKSPACE_NAME,
         google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
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
                       google_billing_project=google_billing_project)
    else:
        copy_to_local(drs_url, dst, workspace_name, google_billing_project)

def copy_batch(drs_urls: Iterable[str],
               dst: str,
               workspace_name: Optional[str]=WORKSPACE_NAME,
               google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    enable_requester_pays(workspace_name, google_billing_project)
    with ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as oneshot_executor:
        oneshot_pool = async_collections.AsyncSet(oneshot_executor, concurrency=IO_CONCURRENCY)
        for drs_url in drs_urls:
            assert drs_url.startswith("drs://")
            src_client, src_info = resolve_drs_for_gs_storage(drs_url)
            src_bucket = src_client.bucket(src_info.bucket_name, user_project=google_billing_project)
            src_blob = src_bucket.get_blob(src_info.key)
            basename = src_info.name or _url_basename(drs_url)
            if dst.startswith("gs://"):
                dst_bucket_name, dst_pfx = _bucket_name_and_key(dst)
                dst_bucket = gs.get_client().bucket(dst_bucket_name)
                dst_key = f"{dst_pfx}/{basename}"
                if MULTIPART_THRESHOLD >= src_blob.size:
                    oneshot_pool.put(gs.oneshot_copy, src_bucket, dst_bucket, src_info.key, dst_key)
                else:
                    gs.multipart_copy(src_bucket, dst_bucket, src_info.key, dst_key)
            else:
                oneshot_pool.put(copy_to_local, drs_url, dst, workspace_name, google_billing_project)
        for _ in oneshot_pool.consume():
            pass

def extract_tar_gz(drs_url: str,
                   dst_pfx: str=None,
                   dst_bucket_name: str=None,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    """
    Extract a `.tar.gz` archive resolved by a DRS url into a Google Storage bucket.
    """
    if dst_bucket_name is None:
        dst_bucket_name = WORKSPACE_BUCKET
    assert dst_bucket_name
    enable_requester_pays(workspace_name, google_billing_project)
    src_client, src_info = resolve_drs_for_gs_storage(drs_url)
    src_bucket = src_client.bucket(src_info.bucket_name, user_project=google_billing_project)
    dst_bucket = gs.get_client().bucket(dst_bucket_name)
    with gscio.Reader(src_bucket.get_blob(src_info.key), threads=2) as fh:
        tar_gz.extract(fh, dst_bucket, root=dst_pfx)

def _url_basename(url: str) -> str:
    schema_re = "[a-z]+://"
    path_re = ".*"
    m = re.match(f"(?P<schema>{schema_re})(?P<path>{path_re})", url)
    if m:
        parts = m['path'].rsplit("/", 1)
        if 2 == len(parts):
            return parts[-1]
        else:
            raise ValueError(f"'{url}' missing basename")
    else:
        raise ValueError(f"'{url}' does not match '{schema_re}{path_re}'")

def _bucket_name_and_key(gs_url: str) -> Tuple[str, str]:
    assert gs_url.startswith("gs://")
    parts = gs_url[5:].split("/", 1)
    if 1 >= len(parts) or not parts[1]:
        raise ValueError(f"Invalid bucket location: {gs_url}"
                         "gs:// url should contain bucket name and key with '/' delimiter.")
    bucket_name, key = parts
    return bucket_name, key
