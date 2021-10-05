#!/usr/bin/env python
import os
import sys
import requests
import unittest
import warnings

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
from tests.infra.server import ThreadedLocalServer, BaseHTTPRequestHandler
from terra_notebook_utils.http import HTTPAdapter, Retry, http_session


class TestHandler(BaseHTTPRequestHandler):
    def do_GET(self, *args, **kwargs):
        self.send_response(500)
        self.end_headers()

    def log_message(self, *args, **kwargs):
        pass

class TestTerraNotebookHTTP(unittest.TestCase):
    def setUp(self):
        # Suppress warnings of the form 'ResourceWarning: unclosed <socket.socket' so they don't muck up test output
        # It'd sure be nice to nice to know how to avoid these things in the first place.
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_retry(self):
        with ThreadedLocalServer(TestHandler):
            expected_recount = 3
            retry_count = dict(count=0)

            class TestRetry(Retry):
                def increment(self, *args, **kwargs):
                    retry_count['count'] += 1
                    return super().increment(*args, **kwargs)

            with http_session(retry=TestRetry(total=expected_recount - 1,
                              status_forcelist=[500],
                              allowed_methods=["GET"])) as http:
                try:
                    http.get("http://localhost:8000")
                except requests.exceptions.RetryError:
                    pass

            self.assertEqual(expected_recount, retry_count['count'])

if __name__ == '__main__':
    unittest.main()
