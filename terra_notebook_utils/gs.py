import io
import os
import json
import threading
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud.storage import Client
from google.oauth2 import service_account
from google.resumable_media.requests import ChunkedDownload
import google.auth

chunk_size = 1024 * 1024 * 32

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

def chunked_download(bucket, key: str):
    blob_name = quote(key, safe="")
    download_url = f"https://storage.googleapis.com/download/storage/v1/b/{bucket.name}/o/{blob_name}?alt=media"
    if bucket.user_project is not None:
        download_url += f"&userProject={bucket.user_project}"
    stream = io.BytesIO()
    download = ChunkedDownload(download_url, chunk_size, stream)
    while not download.finished:
        resp = download.consume_next_chunk(bucket.client._http)
        stream.seek(0)
        yield resp.content

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
    lock = threading.Lock()

    def upload_chunk(chunk, blob_name):
        with lock:
            print("Uploading part", blob_name)
        dst_bucket.blob(blob_name).upload_from_file(io.BytesIO(chunk))

    dst_blob_names: list = list()
    with ThreadPoolExecutor(max_workers=4) as e:
        futures = list()
        for chunk in chunked_download(src_bucket, src_key):
            part_number = len(dst_blob_names) + 1
            blob_name = f"{dst_key}.part{part_number}"
            dst_blob_names.append(blob_name)
            futures.append(e.submit(upload_chunk, chunk, blob_name))
        for f in as_completed(futures):
            f.result()
    _compose_parts(dst_bucket, dst_blob_names, dst_key)

def _iter_chunks(iterable, size=32):
    chunks = list()
    for itm in iterable:
        chunks.append(itm)
        if size == len(chunks):
            yield chunks
            chunks = list()
    if chunks:
        yield chunks

def _compose_parts(bucket, blob_names, dst_key):
    def _compose(names, dst_part_name):
        print("Composing", dst_part_name)
        blobs = [bucket.get_blob(n) for n in names]
        dst_blob = bucket.blob(dst_part_name)
        dst_blob.compose(blobs)
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass

    number_of_blobs = len(blob_names)
    blobs = list()
    while True:
        if 32 >= len(blob_names):
            _compose(blob_names, dst_key)
            break
        else:
            next_blob_names = list()
            chunks = [ch for ch in _iter_chunks(blob_names)]
            for ch in chunks:
                number_of_blobs += 1
                part_name = f"{dst_key}.cpart{number_of_blobs}"
                _compose(ch, part_name)
                next_blob_names.append(part_name)
            blob_names = next_blob_names

def copy(src_bucket, dst_bucket, src_key, dst_key):
    src_blob = src_bucket.blob(src_key)
    src_blob.reload()
    if chunk_size >= src_blob.size:
        oneshot_copy(src_bucket, dst_bucket, src_key, dst_key)
    else:
        multipart_copy(src_bucket, dst_bucket, src_key, dst_key)
    src_blob.reload()
    dst_blob = dst_bucket.get_blob(dst_key)
    assert src_blob.crc32c == dst_blob.crc32c
