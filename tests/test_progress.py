#!/usr/bin/env python
import io
import os
import sys
import time
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import progress
from tests.infra import SuppressWarningsMixin


@testmode("workspace_access")
class TestTerraNotebookUtilsProgress(SuppressWarningsMixin, unittest.TestCase):
    def test_progress_reporter(self):
        with progress.ProgressReporter() as pr:
            pr.checkpoint(2)
            pr.checkpoint(5)
            pr.checkpoint(7)
            time.sleep(2)
            self.assertEqual(14, pr.units_processed)

    def test_rate_limited(self):
        rate = 50
        with self.subTest("Should be rate limited to expected number of calls"):
            call_info = dict(number_of_calls=0)

            @progress.RateLimited(rate)
            def rate_limited_func():
                call_info['number_of_calls'] += 1

            start_time = time.time()
            duration = 0
            while duration <= 4.5 * 1 / rate:
                rate_limited_func()
                duration = time.time() - start_time
            self.assertEqual(5, call_info['number_of_calls'])

        with self.subTest("Should get expected exception for calling too quickly"):
            class MockRateLimitedException(Exception):
                pass

            @progress.RateLimited(rate, MockRateLimitedException)
            def raising_rate_limited_func():
                pass

            with self.assertRaises(MockRateLimitedException):
                for _ in range(2):
                    raising_rate_limited_func()

        with self.subTest("Should be able to avoid rate limit exceptions using reset"):
            for _ in range(2):
                raising_rate_limited_func.reset()
                raising_rate_limited_func()

if __name__ == '__main__':
    unittest.main()
