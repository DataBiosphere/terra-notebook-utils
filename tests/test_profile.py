#!/usr/bin/env python
import io
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests import CLITestMixin
from tests.infra.testmode import testmode
import terra_notebook_utils.cli.commands.profile

@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Profile(CLITestMixin, unittest.TestCase):
    def list_workspace_namespaces(self):
        self._test_cmd(terra_notebook_utils.cli.commands.profile.list_workspace_namespaces)

if __name__ == '__main__':
    unittest.main()
