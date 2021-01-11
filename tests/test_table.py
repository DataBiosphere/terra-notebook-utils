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
    @testmode("controlled_access")
    def test_get_access_token(self):
        gs.get_access_token()

    @testmode("workspace_access")
    def test_table(self):
        table_name = f"test_{uuid4()}"
        number_of_entities = 5
        table.delete(table_name)  # remove cruft from previous failed tests

        id_column = [f"{uuid4()}" for _ in range(number_of_entities)]
        foo_column = [f"{uuid4()}" for _ in range(number_of_entities)]
        bar_column = [f"{uuid4()}" for _ in range(number_of_entities)]

        with self.subTest("Upload table entities"):
            tsv = "\t".join([f"entity:{table_name}_id", "foo", "bar"])
            for i in range(number_of_entities):
                tsv += "\r" + "\t".join((id_column[i], foo_column[i], bar_column[i]))
            table.upload_entities(tsv)

        with self.subTest("delete table"):
            table.delete(table_name)

if __name__ == '__main__':
    unittest.main()
