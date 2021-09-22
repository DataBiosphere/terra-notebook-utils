from typing import BinaryIO

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


default_retry = Retry(total=10,
                      status_forcelist=[429, 500, 502, 503, 504],
                      method_whitelist=["HEAD", "GET"])

def http_session(session: requests.Session=None, retry: Retry=None) -> requests.Session:
    session = session or requests.Session()
    retry = retry or default_retry
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Instantiate a default session. It's useful to have a common session to take advantage of connection pooling.
# Users can modify stuff by replacing this session or instantiate a new one. Note that sessions can be used
# as context managers:
# with http_session(...) as http:
#    http.get(...)
http = http_session()
