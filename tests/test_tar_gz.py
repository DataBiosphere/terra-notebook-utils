#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
import glob
import pytz
import tempfile
from datetime import datetime

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import WORKSPACE_BUCKET
from terra_notebook_utils import gs, tar_gz
from tests.infra import SuppressWarningsMixin


@testmode("workspace_access")
class TestTerraNotebookUtilsTARGZ(SuppressWarningsMixin, unittest.TestCase):
    def test_extract(self):
        with self.subTest("Test tarball extraction to local filesystem"):
            with tempfile.TemporaryDirectory() as tempdir:
                with open("tests/fixtures/test_archive.tar.gz", "rb") as fh:
                    tar_gz.extract(fh, root=tempdir)
                for filename in glob.glob("tests/fixtures/test_archive/*"):
                    with open(filename) as a:
                        with open(os.path.join(f"{tempdir}/test_archive", os.path.basename(filename))) as b:
                            self.assertEqual(a.read(), b.read())
        with self.subTest("Test tarball extraction to GS bucket"):
            start_time = time.time()
            client = gs.get_client()
            bucket = client.bucket(os.environ['WORKSPACE_BUCKET'][5:])
            with open("tests/fixtures/test_archive.tar.gz", "rb") as fh:
                tar_gz.extract(fh, bucket, root="untar_test")
            for filename in glob.glob("tests/fixtures/test_archive/*"):
                key = f"untar_test/test_archive/{os.path.basename(filename)}"
                blob = bucket.get_blob(key)
                self.assertIsNotNone(blob)
                age = (datetime.now(pytz.utc) - blob.time_created).total_seconds()
                self.assertGreater(time.time() - start_time, age)

if __name__ == '__main__':
    unittest.main()
