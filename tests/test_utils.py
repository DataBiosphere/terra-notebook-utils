#!/usr/bin/env python
import os
import sys
import time
import random
import unittest
from unittest import mock
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import utils


@testmode("workspace_access")
class TestUtils(unittest.TestCase):
    def test_concurrent_recursion(self):
        counter = dict()

        def gen_items(number_of_items):
            time.sleep(random.random() / 4)
            counter[f"{uuid4()}"] = "foo"
            n = number_of_items - 1
            res = [n for _ in range(number_of_items) if n]
            return res

        # For an initial value of `3` we expect the following pattern of calls, for a total of 10 calls
        # 3             One call with arument `3`
        # 2 2 2         Three calls with argument `2`
        # 1 1 1 1 1 1   Six calls with argument `1`

        utils.concurrent_recursion(gen_items, {3})
        self.assertEqual(10, len(counter))

    def test_js_get(self):
        data = dict(foo=dict(bar=3))
        self.assertEqual(data['foo'], utils.js_get("foo", data))
        self.assertEqual(data['foo']['bar'], utils.js_get("foo.bar", data))
        self.assertEqual(123, utils.js_get("path.does.not.exist", data, default=123))
        with self.assertRaises(KeyError):
            utils.js_get("foo.not.in.dict", data)

    def test_get_execution_context(self):
        """
        Note: The implementation/logic of this function is likely to change.
        This test currently checks the interim behavior.
        See comments in utils.get_execution_context for more information.

        See this article for info about mock patch for environment variables:
        https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pythons-unittest/
        See this article for info about clearing the @lru_cache when unit testing:
        https://rishabh-ink.medium.com/testing-lru-cache-functions-in-python-with-pytest-33dd5757d11c
        """
        from terra_notebook_utils.utils import get_execution_context, ExecutionContext, \
            ExecutionEnvironment, ExecutionPlatform

        with self.subTest("Typical: Terra, Google"):
            get_execution_context.cache_clear()
            env_vars = {"WORKSPACE_BUCKET": "gs://dummy_bucket/dummy/key/path"}
            with mock.patch.dict(os.environ, env_vars, clear=True):
                exc = get_execution_context()
                self.assertEqual(ExecutionEnvironment.TERRA_WORKSPACE, exc.execution_environment)
                self.assertEqual(ExecutionPlatform.GOOGLE, exc.execution_platform)

        with self.subTest("Current default guess: Terra, Azure"):
            get_execution_context.cache_clear()
            env_vars = {}  # No WORKSPACE_BUCKET in current Azure context
            with mock.patch.dict(os.environ, env_vars, clear=True):
                exc = get_execution_context()
                self.assertEqual(ExecutionEnvironment.TERRA_WORKSPACE, exc.execution_environment)
                self.assertEqual(ExecutionPlatform.AZURE, exc.execution_platform)


if __name__ == '__main__':
    unittest.main()
