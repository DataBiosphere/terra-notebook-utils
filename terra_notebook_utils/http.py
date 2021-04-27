import requests
from typing import BinaryIO
from functools import lru_cache

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
