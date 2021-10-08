import os
import logging
import warnings
import binascii
import collections
import datetime
import hashlib
import google.auth

from typing import Optional
from urllib.parse import quote, urlencode
from google.oauth2 import service_account
from google.cloud.storage import Client

from terra_notebook_utils import TERRA_DEPLOYMENT_ENV
from terra_notebook_utils.logger import logger


logging.getLogger('google.resumable_media.requests.download').setLevel(logger.getEffectiveLevel())
logging.getLogger('gs_chunked_io.writer').setLevel(logger.getEffectiveLevel())

# Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
warnings.filterwarnings('ignore', 'Your application has authenticated using end user credentials')

def get_access_token():
    """Retrieve the access token using the default GCP account returns the same result as
    `gcloud auth print-access-token`.
    """
    if os.environ.get('TERRA_NOTEBOOK_GOOGLE_ACCESS_TOKEN'):
        token = os.environ['TERRA_NOTEBOOK_GOOGLE_ACCESS_TOKEN']
    elif os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
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
        'authorization': f'Bearer {token}',
        'content-type': 'application/json'
    }
    resp = http.delete(f'http://broad-bond-{TERRA_DEPLOYMENT_ENV}.appspot.com/api/link/v1/fence/', headers=headers)
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

def get_signed_url(bucket: str,
                   key: str,
                   sa_credentials: dict = None,
                   requester_pays_user_project: Optional[str] = None):
    """
    Returns V4 signed URLs based on a Google bucket, key, and service account credentials.

    If requester_pays_user_project is included, that project will be billed for requester pays objects.

    See: https://cloud.google.com/storage/docs/access-control/signing-urls-manually

    :param str bucket: The name of the Google bucket
    :param str key: The name of the key referencing the Google bucket object
    :param str sa_credentials: A dictionary containing service account credentials with access to the
                               Google key being referenced.
    :param str requester_pays_user_project: (Optional) Name of a Google project to bill if requester pays.
    """
    default_service_account_credentials = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    # these are the service account credentials returned from Martha
    if sa_credentials:
        creds = service_account.Credentials.from_service_account_info(sa_credentials)
    # if Martha did not give us a service account, try the credentials set at GOOGLE_APPLICATION_CREDENTIALS
    elif default_service_account_credentials:
        creds = service_account.Credentials.from_service_account_file(default_service_account_credentials)
    # we can't access the file
    # TODO: implement signed urls for open access files?
    else:
        raise NotImplementedError(
            '\n   Signed URLs are not currently supported for this DRS URI.\n'
            '   If you have a service account that can access this DRS URI, '
            'setting GOOGLE_APPLICATION_CREDENTIALS should enable this.\n'
            '   See: https://cloud.google.com/docs/authentication/production#passing_variable\n'
        )

    canonical_uri = f'/{quote(key.encode("utf-8"), safe=b"/~")}'

    datetime_now = datetime.datetime.utcnow()
    request_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = datetime_now.strftime('%Y%m%d')

    client_email = creds.service_account_email
    credential_scope = f'{datestamp}/auto/storage/goog4_request'
    credential = f'{client_email}/{credential_scope}'

    host = f'{bucket}.storage.googleapis.com'
    ordered_headers = collections.OrderedDict({'host': host})

    canonical_headers = ''
    for k, v in ordered_headers.items():
        canonical_headers += f'{str(k).lower()}:{str(v).lower()}\n'

    signed_headers = ';'.join([str(k).lower() for k in ordered_headers])

    canonical_query_params = dict()
    canonical_query_params['x-goog-algorithm'] = 'GOOG4-RSA-SHA256'
    canonical_query_params['x-goog-credential'] = credential
    canonical_query_params['x-goog-date'] = request_timestamp
    canonical_query_params['x-goog-expires'] = '3600'
    canonical_query_params['x-goog-signedheaders'] = signed_headers
    if requester_pays_user_project:
        canonical_query_params['userProject'] = requester_pays_user_project

    ordered_query_parameters = collections.OrderedDict(sorted(canonical_query_params.items()))
    canonical_query_string = urlencode(ordered_query_parameters)

    canonical_request = '\n'.join(['GET',
                                   canonical_uri,
                                   canonical_query_string,
                                   canonical_headers,
                                   signed_headers,
                                   'UNSIGNED-PAYLOAD'])
    string_to_sign = '\n'.join(['GOOG4-RSA-SHA256',
                                request_timestamp,
                                credential_scope,
                                hashlib.sha256(canonical_request.encode()).hexdigest()])

    signature = binascii.hexlify(creds.signer.sign(string_to_sign)).decode()
    return f'https://{host}{canonical_uri}?{canonical_query_string}&x-goog-signature={signature}'
