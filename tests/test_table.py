#!/usr/bin/env python
import io
import os
import sys
import unittest
import contextlib
from uuid import uuid4

import gs_chunked_io as gscio

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import drs, table, gs, vcf
from tests.infra import SuppressWarningsMixin


class TestTerraNotebookUtilsTable(SuppressWarningsMixin, unittest.TestCase):
    @testmode("workspace_access")
    def test_fetch_attribute(self):
        table_name = "simple_germline_variation"
        filter_column = "pfb:md5sum"
        filter_val = "d20e5f752a0d55f0a360b7abe1c8499d"
        key = "pfb:object_id"
        val = table.fetch_attribute(table_name, filter_column, filter_val, key)
        self.assertEqual(val, "drs://dg.4503/6e73a376-f7fd-47ed-ac99-0567bb5a5993")

    @testmode("workspace_access")
    def test_fetch_object_id(self):
        with self.subTest("new pfb format (column headers prefixed with 'pfb:')"):
            table_name = "simple_germline_variation"
            file_name = "NWD531899.freeze5.v1.vcf.gz"
            val = table.fetch_object_id(table_name, file_name)
            self.assertEqual(val, "drs://dg.4503/651a4ad1-06b5-4534-bb2c-1f8ed51134f6")
        with self.subTest("old format"):
            table_name = "simple_germline_variation_old_format"
            file_name = "NWD531899.freeze5.v1.vcf.gz"
            val = table.fetch_object_id(table_name, file_name)
            self.assertEqual(val, "drs://dg.4503/651a4ad1-06b5-4534-bb2c-1f8ed51134f6")

    @testmode("controlled_access")
    def test_get_access_token(self):
        gs.get_access_token()

    @testmode("workspace_access")
    def test_print_column(self):
        table_name = "simple_germline_variation"
        column = "pfb:file_name"
        table.print_column(table_name, column)

    @testmode("workspace_access")
    def test_table(self):
        table_name = f"test_{uuid4()}"
        number_of_entities = 5
        table.delete_table(table_name)  # remove cruft from previous failed tests

        id_column = [f"{uuid4()}" for _ in range(number_of_entities)]
        foo_column = [f"{uuid4()}" for _ in range(number_of_entities)]
        bar_column = [f"{uuid4()}" for _ in range(number_of_entities)]

        with self.subTest("Upload table entities"):
            tsv = "\t".join([f"entity:{table_name}_id", "foo", "bar"])
            for i in range(number_of_entities):
                tsv += "\r" + "\t".join((id_column[i], foo_column[i], bar_column[i]))
            table.upload_entities(tsv)

        with self.subTest("list table entities"):
            ents = [e for e in table.list_entities(table_name)]
            self.assertEqual(number_of_entities, len(ents))
            res_foo_column = list()
            res_bar_column = list()
            res_id_column = list()
            for i, e in enumerate(ents):
                res_id_column.append(e['name'])
                res_foo_column.append(e['attributes']['foo'])
                res_bar_column.append(e['attributes']['bar'])
            self.assertEqual(sorted(id_column), sorted(res_id_column))
            self.assertEqual(sorted(foo_column), sorted(res_foo_column))
            self.assertEqual(sorted(bar_column), sorted(res_bar_column))

        with self.subTest("delete table"):
            table.delete_table(table_name)

if __name__ == '__main__':
    unittest.main()
