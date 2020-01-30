#!/usr/bin/env python
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
import terra_notebook_utils
from terra_notebook_utils import drs

class TestTerraNotebookUtils(unittest.TestCase):
    def test_fetch_data_table_attribute(self):
        table = "simple_germline_variation"
        object_name = "4e53b671-cf02-4c0b-94b9-035c4162a2ec"
        key = "object_id"
        val = terra_notebook_utils.fetch_data_table_attribute(table, object_name, key)
        self.assertEqual(val, "drs://dg.4503/695806f6-5bf7-4857-981a-9168b9470b27")

    def test_fetch_data_table_object_id(self):
        table = "simple_germline_variation"
        object_name = "4e53b671-cf02-4c0b-94b9-035c4162a2ec"
        val = terra_notebook_utils.fetch_data_table_object_id(table, object_name)
        self.assertEqual(val, "drs://dg.4503/695806f6-5bf7-4857-981a-9168b9470b27")

    def test_get_access_token(self):
        drs._get_gcp_access_token()

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
