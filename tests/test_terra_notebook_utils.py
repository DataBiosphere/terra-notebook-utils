#!/usr/bin/env python
import os
import sys
import unittest
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
import terra_notebook_utils
from terra_notebook_utils import drs, table, gs

class TestTerraNotebookUtilsTable(unittest.TestCase):
    def test_fetch_attribute(self):
        table_name = "simple_germline_variation"
        filter_column = "name"
        filter_val = "4e53b671-cf02-4c0b-94b9-035c4162a2ec"
        key = "object_id"
        val = table.fetch_attribute(table_name, filter_column, filter_val, key)
        self.assertEqual(val, "drs://dg.4503/695806f6-5bf7-4857-981a-9168b9470b27")

    def test_fetch_object_id(self):
        table_name = "simple_germline_variation"
        file_name = "NWD519795.freeze5.v1.vcf.gz"
        val = table.fetch_object_id(table_name, file_name)
        self.assertEqual(val, "drs://dg.4503/1eee029e-9060-4b56-8d7d-fb96a74d8b42")

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
        data_url, _ = drs._resolve_drs_for_gs_storage(self.drs_url)
        self.assertEqual(data_url, "gs://topmed-irc-share/genomes/NWD522743.b38.irc.v1.cram.crai")

    def test_download(self):
        drs.download(self.drs_url, "foo")

    def test_oneshot_copy(self):
        # This file is too small to trigger multipart copy
        drs.copy("drs://dg.4503/ef88aaaa-ade8-479c-ab26-f72d061f8261", "test_oneshot_object")

    def test_multipart_copy(self):
        # This file is large enough to trigger multipart copy
        drs.copy("drs://dg.4503/6236c17c-b3fa-4d9d-b16f-2e6bef23bd83", "test_multipart_object")

    # Probably don't want to run this test very often. Once a week?
    # Disabled for now
    def _test_multipart_copy_large(self):
        drs.copy("drs://dg.4503/828d82a1-e6cd-4a24-a593-f7e8025c7d71", "test_multipart_object_large")

    def test_compose_parts(self):
        bucket = mock.MagicMock()
        blob_names = [f"part.{i}" for i in range(65)]
        gs._compose_parts(bucket, blob_names, "test_dst_key")

    def test_iter_chunks(self):
        chunk_size = 32
        blob_names = [f"part.{i}" for i in range(65)]
        chunks = [ch for ch in gs._iter_chunks(blob_names, chunk_size)]
        for ch in chunks:
            self.assertEqual(ch, blob_names[:32])
            blob_names = blob_names[32:]

if __name__ == '__main__':
    unittest.main()
