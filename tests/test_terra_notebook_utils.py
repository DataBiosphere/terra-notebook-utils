#!/usr/bin/env python
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
import terra_notebook_utils
from terra_notebook_utils import drs, table

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
        drs._get_gcp_access_token()

    def test_print_column(self):
        table_name = "simple_germline_variation"
        column = "file_name"
        table.print_column(table_name, column)

class TestTerraNotebookUtilsDRS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"

    def test_resolve_drs_for_google_storage(self):
        data_url, _ = drs._resolve_drs_for_google_storage(self.drs_url)
        self.assertEqual(data_url, "gs://topmed-irc-share/genomes/NWD522743.b38.irc.v1.cram.crai")

    def test_download(self):
        drs.download(self.drs_url, "foo")

    def test_copy(self):
        drs.copy(self.drs_url, "test_dst_object")

if __name__ == '__main__':
    unittest.main()
