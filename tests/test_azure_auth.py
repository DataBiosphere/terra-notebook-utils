#!/usr/bin/env python
"""
Test auth for Terra running on Microsoft Azure.
Ideally tests should be run in a Terra Azure workspace.
Unit test this module by mocking as needed to run in a generic context.
"""
import os
import sys
import unittest
from unittest import mock
from unittest.mock import patch
from collections import namedtuple


pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

# from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import azure_auth


@testmode("workspace_access")
class TestAzureAuth(unittest.TestCase):
    def test_get_azure_access_token(self):
        """
         See this article for info about mock patch for environment variables:
         https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pythons-unittest/
        """
        dummy_token_value = "@@DUMMY_AZURE_ACCESS_TOKEN@@"

        # Test token that is explicitly provided by env var "TERRA_NOTEBOOK_AZURE_ACCESS_TOKEN"
        with self.subTest("Azure token provided as env var"):
            env_vars = {"TERRA_NOTEBOOK_AZURE_ACCESS_TOKEN": dummy_token_value}
            with mock.patch.dict(os.environ, env_vars, clear=True):
                self.assertEqual(dummy_token_value, azure_auth.get_azure_access_token())

        # Test getting Azure default credentials token
        with self.subTest("Get Azure default credentials access token"):
            AzureToken = namedtuple("AzureToken", "token")
            azure_token = AzureToken(dummy_token_value)
            with patch.object(azure_auth.DefaultAzureCredential, "get_token", return_value=azure_token):
                self.assertEqual(dummy_token_value, azure_auth.get_azure_access_token())

        # Verify that the default credential is managed as a singleton
        with self.subTest("Verify DefaultAzureCredential singleton"):
            azure_auth._AZURE_CREDENTIAL = None
            first_value = azure_auth._get_default_credential()
            second_value = azure_auth._get_default_credential()
            self.assertIs(first_value, second_value)


if __name__ == '__main__':
    unittest.main()
