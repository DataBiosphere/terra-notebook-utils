#!/usr/bin/env python
import os
import sys
import time
import random
import unittest
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
        with self.assertRaises(KeyError):
            utils.js_get("foo.not.in.dict", data)

if __name__ == '__main__':
    unittest.main()
