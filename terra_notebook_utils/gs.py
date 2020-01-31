import io
from urllib.parse import quote

from google.cloud.storage import Client
from google.oauth2 import service_account
from google.resumable_media.requests import ChunkedDownload

chunk_size = 1024 * 1024 * 4

def get_access_token():
    """
    Retriev the access token using the default GCP account
    returns the same result as `gcloud auth print-access-token`
    """
    import google.auth.transport.requests
    creds, projects = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token

def get_client(credentials_data: dict=None, project: str=None):
    kwargs = dict()
    if credentials_data is not None:
        creds = service_account.Credentials.from_service_account_info(credentials_data)
        kwargs['credentials'] = creds
    if project is not None:
        kwargs['project'] = project
    return Client(**kwargs)

def iter_download(client, bucket_name: str, key: str, user_project: str=None):
    blob_name = quote(key, safe="")
    download_url = f"https://storage.googleapis.com/download/storage/v1/b/{bucket_name}/o/{blob_name}?alt=media"
    if user_project is not None:
        download_url += f"&userProject={user_project}"
    stream = io.BytesIO()
    download = ChunkedDownload(download_url, chunk_size, stream)
    while not download.finished:
        resp = download.consume_next_chunk(client._http)
        stream.seek(0)
        yield resp.content

def compose_parts(bucket, blob_names, dst_key):
    blobs = [bucket.get_blob(b) for b in blob_names]
    dst_blob = bucket.blob(dst_key)
    dst_blob.compose(blobs)
    for blob in blobs:
        try:
            blob.delete()
        except Exception:
            pass

def oneshot_copy():
    pass

def multipart_copy():
    pass
