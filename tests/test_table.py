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
from terra_notebook_utils import table as tnu_table
from tests.infra import SuppressWarningsMixin


class TestTerraNotebookUtilsTable(SuppressWarningsMixin, unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        for table in tnu_table.list_tables():
            if table.startswith("test-table"):
                tnu_table.delete(table)

    @testmode("workspace_access")
    def test_table(self):
        table = f"test-table-{uuid4()}"
        expected_rows = self._gen_rows(997)

        with self.subTest("build table"):
            start_time = time.time()
            tnu_table.put_rows(table, expected_rows.values())
            print("BUILD DURATION", time.time() - start_time)

        with self.subTest("get table"):
            fetched_rows = {row.name: row for row in tnu_table.list_rows(table)}
            for row_name in expected_rows:
                a = expected_rows[row_name]
                b = fetched_rows.get(row_name)
                if a != b:
                    print(a)
                    print(b)
                    print()
            self.assertEqual(expected_rows, fetched_rows)

        with self.subTest("destroy_table"):
            start_time = time.time()
            tnu_table.delete(table)
            print("DESTROY DURATION", time.time() - start_time)
            fetched_rows = [row for row in tnu_table.list_rows(table)]
            self.assertEqual(0, len(fetched_rows))

    def _gen_rows(self, number_of_rows: int):
        def _rand_value(types={str, int, float, bool, list}):
            t = random.choice(list(types))
            if str == t:
                val = f"{uuid4()}"
            elif int == t:
                val = random.randint(0, 997)
                val *= (-1 + random.choice((0, 2)))
            elif float == t:
                val = random.random()
            elif bool == t:
                val = bool(random.randint(0, 1))
            elif list == t:
                list_member_type = random.choice(list(types - {list}))
                val = [_rand_value([list_member_type]) for _ in range(random.randint(1, 4))]
            return val

        all_column_headers = [["foo", "bar", "fubar", "snafu"], ["foo", "bar"], ["bar"]]
        rows = dict()
        for _ in range(number_of_rows):
            column_headers = random.choice(all_column_headers)
            attributes = {c: _rand_value() for c in column_headers}
            row_name = f"{uuid4()}"
            rows[row_name] = tnu_table.Row(row_name, attributes)
        return rows

    @testmode("workspace_access")
    def test_put_get_delete_types(self):
        table = f"test-table-{uuid4()}"

        row = tnu_table.Row(name=f"{uuid4()}",
                            attributes=dict(foo=f"{uuid4()}", bar=f"{uuid4()}"))
        with self.subTest("put", type="row"):
            tnu_table.put_row(table, row)
            self.assertEqual(row, tnu_table.get_row(table, row))
        with self.subTest("del", type="row"):
            tnu_table.del_row(table, row)
            self.assertIsNone(tnu_table.get_row(table, row))
            tnu_table.del_row(table, row)  # this should work twice

        attributes = dict(foo=f"{uuid4()}", bar=f"{uuid4()}")
        with self.subTest("put", type="dict"):
            name = tnu_table.put_row(table, attributes)
            self.assertEqual(attributes, tnu_table.get_row(table, name).attributes)
        with self.subTest("del", type="dict"):
            tnu_table.del_row(table, name)
            self.assertIsNone(tnu_table.get_row(table, name))
            tnu_table.del_row(table, name)  # this should work twice

    @testmode("workspace_access")
    def test_fetch_drs_url(self):
        file_name = f"{uuid4()}"
        drs_uri = f"drs://{uuid4()}"

        with self.subTest("new pfb format (column headers prefixed with 'pfb:')"):
            table = f"test-table-{uuid4()}"
            tnu_table.put_row(table, {'pfb:file_name': file_name, 'pfb:object_id': drs_uri})
            val = tnu_table.fetch_drs_url(table, file_name)
            self.assertEqual(val, drs_uri)

        with self.subTest("old format"):
            table = f"test-table-{uuid4()}"
            tnu_table.put_row(table, {'file_name': file_name, 'object_id': drs_uri})
            val = tnu_table.fetch_drs_url(table, file_name)
            self.assertEqual(val, drs_uri)

if __name__ == '__main__':
    unittest.main()
