#!/usr/bin/env python
"""
Test getting an access token for use with Terra backend services.
This may be either a Google or Azure access token, depending on the execution context.
Ideally tests should be run in a Terra Azure workspace.
Unit test this module by mocking as needed to run in a generic context.
"""
import os
import sys
import unittest
import warnings
from collections import namedtuple
from unittest import mock
from unittest.mock import patch

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import azure.core.exceptions
import google.auth.exceptions

# from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import azure_auth, terra_auth
from terra_notebook_utils.utils import ExecutionContext, ExecutionEnvironment, ExecutionPlatform, get_execution_context


@testmode("workspace_access")
class TestTerraAuth(unittest.TestCase):
    AzureToken = namedtuple("AzureToken", "token")
    dummy_azure_access_token_value = "@@DUMMY_AZURE_ACCESS_TOKEN@@"
    dummy_google_access_token_value = "@@DUMMY_GOOGLE_ACCESS_TOKEN@@"

    def setUp(self):
        # Suppress warnings of the form 'ResourceWarning: unclosed <socket.socket' so they don't muck up test output
        # It'd sure be nice to know how to avoid these things in the first place.
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_get_terra_access_token_in_terra_google_context(self):
        """
         See this article for info about mock patch for environment variables:
         https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pythons-unittest/
         """
        get_execution_context.cache_clear()
        env_vars = {"WORKSPACE_BUCKET": "gs://dummy_bucket/dummy/key/path"}
        with mock.patch.dict(os.environ, env_vars, clear=True):
            # Force fresh identification of the execution context performed in the constructor
            terra_auth.TERRA_AUTH_TOKEN_PROVIDER = terra_auth.TerraAuthTokenProvider()
            with patch.object(terra_auth.gs, "get_access_token", return_value=self.dummy_google_access_token_value):
                self.assertEqual(self.dummy_google_access_token_value, terra_auth.get_terra_access_token())

    def test_get_terra_access_token_in_terra_azure_context(self):
        """
        See this article for info about mock patch for environment variables:
        https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pythons-unittest/
        """
        get_execution_context.cache_clear()
        env_vars = {}  # No WORKSPACE_BUCKET in current Azure context
        with mock.patch.dict(os.environ, env_vars, clear=True):
            # Force fresh identification of the execution context performed in the constructor
            terra_auth.TERRA_AUTH_TOKEN_PROVIDER = terra_auth.TerraAuthTokenProvider()
            azure_token = self.AzureToken(self.dummy_azure_access_token_value)
            with patch.object(azure_auth.DefaultAzureCredential, "get_token", return_value=azure_token):
                self.assertEqual(self.dummy_azure_access_token_value, terra_auth.get_terra_access_token())

    def test_get_terra_access_token_in_unknown_context_with_google_auth_available(self):
        with patch.object(terra_auth, "get_execution_context",
                          return_value=ExecutionContext(ExecutionEnvironment.OTHER, ExecutionPlatform.UNKNOWN)):
            # Force fresh identification of the execution context performed in the constructor
            terra_auth.TERRA_AUTH_TOKEN_PROVIDER = terra_auth.TerraAuthTokenProvider()
            with patch.object(terra_auth.gs, "get_access_token", return_value=self.dummy_google_access_token_value):
                with patch.object(azure_auth, "get_azure_access_token",
                                  side_effect=azure.core.exceptions.ClientAuthenticationError):
                    self.assertEqual(self.dummy_google_access_token_value, terra_auth.get_terra_access_token())

    def test_get_terra_access_token_in_unknown_context_with_azure_auth_available(self):
        with patch.object(terra_auth, "get_execution_context",
                          return_value=ExecutionContext(ExecutionEnvironment.OTHER, ExecutionPlatform.UNKNOWN)):
            # Force fresh identification of the execution context performed in the constructor
            terra_auth.TERRA_AUTH_TOKEN_PROVIDER = terra_auth.TerraAuthTokenProvider()
            with patch.object(terra_auth.gs, "get_access_token",
                              side_effect=google.auth.exceptions.DefaultCredentialsError):
                azure_token = self.AzureToken(self.dummy_azure_access_token_value)
                with patch.object(azure_auth.DefaultAzureCredential, "get_token", return_value=azure_token):
                    self.assertEqual(self.dummy_azure_access_token_value, terra_auth.get_terra_access_token())


if __name__ == '__main__':
    unittest.main()
