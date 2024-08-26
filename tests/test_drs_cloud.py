#!/usr/bin/env python
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from terra_notebook_utils import drs


#  To run these tests: `python -m unittest tests/test_drs_cloud.py`
class TestDRS(unittest.TestCase):
    def test_passing_through_cloud_platform(self):
        public_drs_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_e2151834-13cd-4156-9ea2-168a1b7abf60_0761203d-d2a1-448e-8f71-9f81d80ddd9d"
        fields = ["fileName",
                  "hashes",
                  "size",
                  "gsUri",
                  "bucket",
                  "name",
                  "timeUpdated",
                  "googleServiceAccount",
                  "accessUrl"]
        resp = drs.get_drs(public_drs_url, fields=fields)
        self.assertEqual(resp.status_code, 200)

    def test_get_drs_cloud_platform(self):
        cloud_platform = drs.get_drs_cloud_platform()
        self.assertEqual(cloud_platform, "gs")


if __name__ == '__main__':
    unittest.main()
