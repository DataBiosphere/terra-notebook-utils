import os
import logging
import warnings

from google.cloud.storage import Client
from google.oauth2 import service_account
import google.auth

from terra_notebook_utils import TERRA_DEPLOYMENT_ENV
from terra_notebook_utils.logger import logger


logging.getLogger("google.resumable_media.requests.download").setLevel(logger.getEffectiveLevel())
logging.getLogger("gs_chunked_io.writer").setLevel(logger.getEffectiveLevel())

# Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

def get_access_token():
    """Retrieve the access token using the default GCP account returns the same result as
    `gcloud auth print-access-token`.
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
    from terra_notebook_utils.http import http
    token = get_access_token()
    headers = {
        'authorization': f"Bearer {token}",
        'content-type': "application/json"
    }
    resp = http.delete(f"http://broad-bond-{TERRA_DEPLOYMENT_ENV}.appspot.com/api/link/v1/fence/", headers=headers)
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
