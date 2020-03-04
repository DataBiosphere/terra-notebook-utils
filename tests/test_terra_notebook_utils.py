#!/usr/bin/env python
import os
import sys
import time
import unittest
import glob
import pytz
from datetime import datetime
from unittest import mock

import gs_chunked_io as gscio

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
import terra_notebook_utils
from terra_notebook_utils import drs, table, gs, tar_gz

class TestTerraNotebookUtilsTable(unittest.TestCase):
    def test_fetch_attribute(self):
        table_name = "simple_germline_variation"
        filter_column = "name"
        filter_val = "74b42836-16ab-4bf7-a683-dc5e603d0bc7"
        key = "object_id"
        val = table.fetch_attribute(table_name, filter_column, filter_val, key)
        self.assertEqual(val, "drs://dg.4503/6e73a376-f7fd-47ed-ac99-0567bb5a5993")

    def test_fetch_object_id(self):
        table_name = "simple_germline_variation"
        file_name = "NWD531899.freeze5.v1.vcf.gz"
        val = table.fetch_object_id(table_name, file_name)
        self.assertEqual(val, "drs://dg.4503/651a4ad1-06b5-4534-bb2c-1f8ed51134f6")

    def test_get_access_token(self):
        gs.get_access_token()

    def test_print_column(self):
        table_name = "simple_germline_variation"
        column = "file_name"
        table.print_column(table_name, column)

class TestTerraNotebookUtilsDRS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"

    def test_resolve_drs_for_google_storage(self):
        _, bucket_name, key = drs.resolve_drs_for_gs_storage(self.drs_url)
        self.assertEqual(bucket_name, "topmed-irc-share")
        self.assertEqual(key, "genomes/NWD522743.b38.irc.v1.cram.crai")

    def test_download(self):
        drs.download(self.drs_url, "foo")

    def test_oneshot_copy(self):
        drs.copy(self.drs_url, "test_oneshot_object")

    def test_multipart_copy(self):
        drs.copy(self.drs_url, "test_oneshot_object", multipart_threshold=1024 * 1024)

    # Probably don't want to run this test very often. Once a week?
    def _test_multipart_copy_large(self):
        drs_url = "drs://dg.4503/828d82a1-e6cd-4a24-a593-f7e8025c7d71"
        drs.copy(drs_url, "test_multipart_object_large")

    # Probably don't want to run this test very often. Once a week?
    def _test_extract_tar_gz(self):
        drs_url = "drs://dg.4503/273f3453-4d16-4ddd-8877-dbac958a4f4d"  # Amish cohort v4 VCF
        drs.extract_tar_gz(drs_url, "test_cohort_extract")

class TestTerraNotebookUtilsTARGZ(unittest.TestCase):
    def test_extract(self):
        with self.subTest("Test tarball extraction to local filesystem"):
            with open("tests/fixtures/test_archive.tar.gz", "rb") as fh:
                tar_gz.extract(fh, root="untar_test")
            for filename in glob.glob("tests/fixtures/test_archive/*"):
                with open(filename) as a:
                    with open(os.path.join("untar_test/test_archive", os.path.basename(filename))) as b:
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
