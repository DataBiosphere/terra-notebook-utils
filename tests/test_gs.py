#!/usr/bin/env python
import io
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import gs
from tests.infra import SuppressWarningsMixin


@testmode("workspace_access")
class TestTerraNotebookUtilsGS(SuppressWarningsMixin, unittest.TestCase):
    def test_get_access_token(self):
        gs.get_access_token()

    def test_reset_ecm_cache(self):
        gs.reset_ecm_cache()

    def test_get_client(self):
        gs.get_client()

if __name__ == '__main__':
    unittest.main()
