#!/usr/bin/env python
import io
import os
import sys
import json
import typing
import base64
import unittest
import argparse
from unittest import mock
from random import randint
from uuid import uuid4
from contextlib import redirect_stdout
from tempfile import NamedTemporaryFile

import crc32c

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import TestCaseSuppressWarnings, config
from tests.infra.testmode import testmode
from terra_notebook_utils import gs, WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET
from terra_notebook_utils.cli import Config
import terra_notebook_utils.cli
import terra_notebook_utils.cli.config
import terra_notebook_utils.cli.vcf
import terra_notebook_utils.cli.workspace
import terra_notebook_utils.cli.profile
import terra_notebook_utils.cli.drs
import terra_notebook_utils.cli.table


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Config(TestCaseSuppressWarnings):
    def test_config_print(self):
        workspace = f"{uuid4()}"
        workspace_google_project = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            with _ConfigOverride(workspace, workspace_google_project, tf.name):
                Config.write()
                args = argparse.Namespace()
                out = io.StringIO()
                with redirect_stdout(out):
                    terra_notebook_utils.cli.config.config_print(args)
                data = json.loads(out.getvalue())
                self.assertEqual(data, dict(workspace=workspace, workspace_google_project=workspace_google_project))

    def test_resolve(self):
        with self.subTest("Should fall back to env vars if arguments are None and config file missing"):
            with _ConfigOverride(None, None):
                workspace, namespace = Config.resolve(None, None)
                self.assertEqual(WORKSPACE_NAME, workspace)
                self.assertEqual(WORKSPACE_GOOGLE_PROJECT, namespace)
        with self.subTest("Should fall back to config if arguments are None/False"):
            with _ConfigOverride(str(uuid4()), str(uuid4())):
                workspace, namespace = Config.resolve(None, None)
                self.assertEqual(Config.info['workspace'], workspace)
                self.assertEqual(Config.info['workspace_google_project'], namespace)
        with self.subTest("Should attempt namespace resolve via fiss when workspace present, namespace empty"):
            expected_namespace = str(uuid4())
            with mock.patch("terra_notebook_utils.workspace.get_workspace_namespace", return_value=expected_namespace):
                with _ConfigOverride(WORKSPACE_NAME, None):
                    terra_notebook_utils.cli.WORKSPACE_GOOGLE_PROJECT = None
                    workspace, namespace = Config.resolve(None, None)
                    self.assertEqual(Config.info['workspace'], workspace)
                    self.assertEqual(expected_namespace, namespace)
        with self.subTest("Should attempt namespace resolve via fiss when workspace present, namespace empty"):
            expected_workspace = str(uuid4())
            expected_namespace = str(uuid4())
            with mock.patch("terra_notebook_utils.workspace.get_workspace_namespace", return_value=expected_namespace):
                with _ConfigOverride(str(uuid4()), str(uuid4())):
                    terra_notebook_utils.cli.WORKSPACE_GOOGLE_PROJECT = None
                    workspace, namespace = Config.resolve(expected_workspace, expected_namespace)
                    self.assertEqual(expected_workspace, workspace)
                    self.assertEqual(expected_namespace, namespace)

    def test_config_set(self):
        new_workspace = f"{uuid4()}"
        new_workspace_google_project = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            with _ConfigOverride(None, None, tf.name):
                Config.write()
                args = argparse.Namespace(workspace=new_workspace)
                terra_notebook_utils.cli.config.set_config_workspace(args)
                args = argparse.Namespace(billing_project=new_workspace_google_project)
                terra_notebook_utils.cli.config.set_config_billing_project(args)
                with open(tf.name) as fh:
                    data = json.loads(fh.read())
                self.assertEqual(data, dict(workspace=new_workspace,
                                            workspace_google_project=new_workspace_google_project))

class _CLITestCase(TestCaseSuppressWarnings):
    common_kwargs: dict = dict()

    def _test_cmd(self, cmd: typing.Callable, **kwargs):
        with NamedTemporaryFile() as tf:
            with _ConfigOverride(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, tf.name):
                Config.write()
                args = argparse.Namespace(**dict(**self.common_kwargs, **kwargs))
                out = io.StringIO()
                with redirect_stdout(out):
                    cmd(args)
                return out.getvalue().strip()


class TestTerraNotebookUtilsCLI_VCF(_CLITestCase):
    common_kwargs = dict(google_billing_project=WORKSPACE_GOOGLE_PROJECT)
    vcf_drs_url = "drs://dg.4503/57f58130-2d66-4d46-9b2b-539f7e6c2080"

    @testmode("workspace_access")
    def test_head(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head,
                           path="gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
                                "/consent1"
                                "/HVH_phs000993_TOPMed_WGS_freeze.8.chr10.hg38.vcf.gz")

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_head_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path=self.vcf_drs_url)

    @testmode("workspace_access")
    def test_samples(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples,
                           path="gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
                                "/consent1"
                                "/HVH_phs000993_TOPMed_WGS_freeze.8.chr10.hg38.vcf.gz")

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_samples_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples,
                           path=self.vcf_drs_url)

    def test_stats(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats,
                           path="gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
                                "/consent1"
                                "/HVH_phs000993_TOPMed_WGS_freeze.8.chr10.hg38.vcf.gz")

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_stats_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats,
                           path=self.vcf_drs_url)


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Workspace(_CLITestCase):
    def test_list(self):
        self._test_cmd(terra_notebook_utils.cli.workspace.list_workspaces)

    def test_get(self):
        self._test_cmd(terra_notebook_utils.cli.workspace.get_workspace,
                       workspace=WORKSPACE_NAME,
                       namespace="firecloud-cgl")


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Profile(_CLITestCase):
    def test_list_billing_projects(self):
        self._test_cmd(terra_notebook_utils.cli.profile.list_billing_projects)


@testmode("controlled_access")
class TestTerraNotebookUtilsCLI_DRS(_CLITestCase):
    drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"
    expected_crc32c = "LE1Syw=="

    def test_copy(self):
        with self.subTest("test local"):
            with NamedTemporaryFile() as tf:
                self._test_cmd(terra_notebook_utils.cli.drs.drs_copy,
                               drs_url=self.drs_url,
                               dst=tf.name,
                               google_billing_project=WORKSPACE_GOOGLE_PROJECT)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test gs"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.drs.drs_copy,
                           drs_url=self.drs_url,
                           dst=f"gs://{WORKSPACE_BUCKET}/{key}",
                           google_billing_project=WORKSPACE_GOOGLE_PROJECT)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
            out = io.BytesIO()
            blob.download_to_file(out)
            self.assertEqual(self.expected_crc32c, blob.crc32c)
            self.assertEqual(_crc32c(out.getvalue()), blob.crc32c)


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Table(_CLITestCase):
    common_kwargs = dict(workspace=WORKSPACE_NAME, namespace=WORKSPACE_GOOGLE_PROJECT)

    @classmethod
    def setUpClass(cls):
        cls.table = "simple_germline_variation"
        with open("tests/fixtures/workspace_manifest.json") as fh:
            cls.table_data = json.loads(fh.read(), parse_int=str)[cls.table]
        cls.columns = list(cls.table_data[0].keys())
        cls.columns.remove("entity_id")

    def setUp(self):
        self.row_index = randint(0, len(self.table_data) - 1)
        self.entity_id = self.table_data[self.row_index]['entity_id']
        self.column = self.columns[randint(0, len(self.columns) - 1)]
        self.cell_value = self.table_data[self.row_index][self.column]

    def test_list(self):
        self._test_cmd(terra_notebook_utils.cli.table.list_tables)

    def test_get(self):
        self._test_cmd(terra_notebook_utils.cli.table.get_table, table="simple_germline_variation")

    def test_get_row(self):
        out = self._test_cmd(terra_notebook_utils.cli.table.get_row,
                             table=self.table,
                             id=self.entity_id)
        row = json.loads(out)
        row['entity_id'] = row.pop(f"{self.table}_id")
        self.assertEqual(row, self.table_data[self.row_index])

    def test_get_cell(self):
        column_index = randint(0, len(self.columns) - 1)
        column = self.columns[column_index]
        out = self._test_cmd(terra_notebook_utils.cli.table.get_cell,
                             table=self.table,
                             id=self.entity_id,
                             column=column)
        self.assertEqual(self.table_data[self.row_index][column], out)

class _ConfigOverride:
    def __init__(self, workspace, namespace, path=None):
        self.old_workspace = Config.info['workspace']
        self.old_namespace = Config.info['workspace_google_project']
        self.old_path = Config.path
        self.workspace = workspace
        self.namespace = namespace
        self.path = path

    def __enter__(self):
        Config.info['workspace'] = self.workspace
        Config.info['workspace_google_project'] = self.namespace
        Config.path = self.path

    def __exit__(self, *args, **kwargs):
        Config.info['workspace'] = self.old_workspace
        Config.info['workspace_google_project'] = self.old_namespace
        Config.path = self.old_path

def _crc32c(data: bytes) -> str:
    # Compute Google's wonky base64 encoded crc32c checksum
    return base64.b64encode(crc32c.Checksum(data).digest()).decode("utf-8")


if __name__ == '__main__':
    unittest.main()
