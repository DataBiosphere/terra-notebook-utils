import io
import os
import sys
import json
import math
import typing
import logging
import threading
from contextlib import closing

from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
from google.oauth2 import service_account
import google.auth

from terra_notebook_utils.progress_bar import ProgressBar

logging.getLogger("google.resumable_media.requests.download").setLevel(logging.WARNING)

_default_chunk_size = 32 * 1024 * 1024
_gs_max_parts_per_compose = 32

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

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

def reset_bond_cache():
    import requests
    token = get_access_token()
    headers = {
        'authorization': f"Bearer {token}",
        'content-type': "application/json"
    }
    resp = requests.delete("http://broad-bond-prod.appspot.com/api/link/v1/fence/", headers=headers)
    print(resp.content)

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

class ChunkedReader:
    def __init__(self, blob: Blob, chunk_size: int=_default_chunk_size):
        self.blob = blob
        self._chunk_size = chunk_size
        self.part_numbers = list(range(math.ceil(blob.size / self._chunk_size)))

        self._buffer: bytes = None
        self._current_part_number: int = None
        self._executor: typing.Optional[ThreadPoolExecutor] = None
        self._futures: typing.Optional[list] = None
        self._read_forward_factor = 3

    def fetch_part(self, part_number: int):
        start_chunk = part_number * self._chunk_size
        end_chunk = start_chunk + self._chunk_size - 1
        fh = io.BytesIO()
        self.blob.download_to_file(fh, start=start_chunk, end=end_chunk)
        fh.seek(0)
        return fh.read()

    def _number_of_parts_buffered(self):
        return len(self._buffer) // self._chunk_size + len(self._futures)

    def read(self, k_bytes: int) -> bytes:
        if self._buffer is None:
            self._buffer = bytes()
            self._current_part_number = 0
            self._executor = ThreadPoolExecutor(max_workers=4)
            self._futures = list()

        if self._current_part_number <= self.part_numbers[-1]:
            while self._number_of_parts_buffered() < self._read_forward_factor:
                f = self._executor.submit(self.fetch_part, self._current_part_number)
                self._futures.append(f)
                self._current_part_number += 1

        while len(self._buffer) < k_bytes and len(self._futures):
            for f in as_completed(self._futures[:1]):
                self._buffer += self._futures[0].result()
                del self._futures[0]

        ret_data = self._buffer[:k_bytes]
        self._buffer = self._buffer[k_bytes:]
        return ret_data

    def close(self):
        pass

class ChunkedWriter:
    def __init__(self, key: str, bucket: Bucket, chunk_size: int=_default_chunk_size):
        self.key = key
        self.bucket = bucket
        self._chunk_size = chunk_size
        self._part_names: typing.List[str] = list()
        self._buffer: bytes = None
        self._current_part_number: int = None

        self._executor: typing.Optional[ThreadPoolExecutor] = None
        self._futures: typing.Optional[set] = None

    def put_part(self, part_number: int, data: bytes):
        part_name = self._compose_part_name(part_number)
        self.bucket.blob(part_name).upload_from_file(io.BytesIO(data))
        self._part_names.append(part_name)
        logger.info(f"Uploaded part {part_name}, size={len(data)}")

    def write(self, data: bytes):
        if self._buffer is None:
            self._buffer = bytes()
            self._current_part_number = 0
            self._executor = ThreadPoolExecutor(max_workers=4)
            self._futures = set()
        self._buffer += data
        if len(self._buffer) >= self._chunk_size:
            f = self._executor.submit(self.put_part, self._current_part_number, self._buffer[:self._chunk_size])
            self._futures.add(f)
            self._buffer = self._buffer[self._chunk_size:]
            self._current_part_number += 1

        for f in self._futures.copy():
            if f.done():
                self._futures.remove(f)

    def close(self):
        if self._buffer is not None:
            if len(self._buffer):
                self.put_part(self._current_part_number, self._buffer)
            for f in as_completed(self._futures):
                pass
        part_names = sorted(self._part_names.copy())
        part_numbers = [len(part_names)]
        while True:
            if _gs_max_parts_per_compose >= len(part_names):
                self._compose_parts(part_names, self.key)
                break
            else:
                chunks = [ch for ch in _iter_chunks(part_names)]
                part_numbers = range(part_numbers[-1], part_numbers[-1] + len(chunks))
                with ThreadPoolExecutor(max_workers=8) as e:
                    futures = [e.submit(self._compose_parts, ch, self._compose_part_name(part_number))
                               for ch, part_number in zip(chunks, part_numbers)]
                    part_names = sorted([f.result() for f in as_completed(futures)])

    def _compose_parts(self, part_names, dst_part_name):
        blobs = list()
        for name in part_names:
            blob = self.bucket.get_blob(name)
            if blob is None:
                msg = f"No blob found for bucket={self.bucket.name} name={name}"
                logger.error(msg)
                raise Exception(msg)
            blobs.append(blob)
        dst_blob = self.bucket.blob(dst_part_name)
        dst_blob.compose(blobs)
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass
        return dst_part_name

    def _compose_part_name(self, part_number):
        return "%s.part%06i" % (self.key, part_number)

def _iter_chunks(lst: list, size=32):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

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
    src_blob = src_bucket.get_blob(src_key)
    print(f"Copying from {src_bucket.name}/{src_key}")
    print(f"Copying to {dst_bucket.name}/{dst_key}")
    reader = ChunkedReader(src_blob)
    writer = ChunkedWriter(dst_key, dst_bucket)
    progress_bar = ProgressBar(len(reader.part_numbers) + 1,
                               prefix="Copying:",
                               size=src_blob.size // 1024 ** 2,
                               units="MB")

    def _transfer_chunk(part_number):
        data = reader.fetch_part(part_number)
        writer.put_part(part_number, data)
        progress_bar.update()

    with closing(progress_bar):
        with ThreadPoolExecutor(max_workers=3) as e:
            futures = [e.submit(_transfer_chunk, part_number) for part_number in reader.part_numbers]
            for f in as_completed(futures):
                pass
        writer.close()
        progress_bar.update()

def copy(src_bucket, dst_bucket, src_key, dst_key):
    src_blob = src_bucket.blob(src_key)
    src_blob.reload()
    if _default_chunk_size >= src_blob.size:
        oneshot_copy(src_bucket, dst_bucket, src_key, dst_key)
    else:
        multipart_copy(src_bucket, dst_bucket, src_key, dst_key)
    src_blob.reload()
    dst_blob = dst_bucket.get_blob(dst_key)
    assert src_blob.crc32c == dst_blob.crc32c
