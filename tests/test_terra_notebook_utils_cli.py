#!/usr/bin/env python
import io
import os
import sys
import json
import typing
import base64
import unittest
import argparse
import subprocess
import traceback
from unittest import mock
from random import randint
from uuid import uuid4
from contextlib import redirect_stdout
from tempfile import NamedTemporaryFile
from typing import List

import google_crc32c

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests import CLITestMixin, ConfigOverride
from tests.infra.testmode import testmode
from terra_notebook_utils import gs, table, WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET
from terra_notebook_utils.cli import Config
import terra_notebook_utils.cli
import terra_notebook_utils.cli.config
import terra_notebook_utils.cli.vcf
import terra_notebook_utils.cli.workspace
import terra_notebook_utils.cli.profile
import terra_notebook_utils.cli.drs
import terra_notebook_utils.cli.table
from tests.infra import SuppressWarningsMixin, upload_data
from tests.infra.partialize_vcf import partialize_vcf


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Config(SuppressWarningsMixin, unittest.TestCase):
    def test_config_print(self):
        workspace = f"{uuid4()}"
        workspace_namespace = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            with ConfigOverride(workspace, workspace_namespace, tf.name):
                Config.write()
                args = argparse.Namespace()
                out = io.StringIO()
                with redirect_stdout(out):
                    terra_notebook_utils.cli.config.config_print(args)
                data = json.loads(out.getvalue())
                self.assertEqual(data, dict(workspace=workspace, workspace_namespace=workspace_namespace))

    def test_resolve(self):
        with self.subTest("Should fall back to env vars if arguments are None and config file missing"):
            with ConfigOverride(None, None):
                workspace, namespace = Config.resolve(None, None)
                self.assertEqual(WORKSPACE_NAME, workspace)
                self.assertEqual(WORKSPACE_GOOGLE_PROJECT, namespace)
        with self.subTest("Should fall back to config if arguments are None/False"):
            with ConfigOverride(str(uuid4()), str(uuid4())):
                workspace, namespace = Config.resolve(None, None)
                self.assertEqual(Config.info['workspace'], workspace)
                self.assertEqual(Config.info['workspace_namespace'], namespace)
        with self.subTest("Should attempt namespace resolve via fiss when workspace present, namespace empty"):
            expected_namespace = str(uuid4())
            with mock.patch("terra_notebook_utils.workspace.get_workspace_namespace", return_value=expected_namespace):
                with ConfigOverride(WORKSPACE_NAME, None):
                    terra_notebook_utils.cli.WORKSPACE_GOOGLE_PROJECT = None
                    workspace, namespace = Config.resolve(None, None)
                    self.assertEqual(Config.info['workspace'], workspace)
                    self.assertEqual(expected_namespace, namespace)
        with self.subTest("Should use overrides for workspace and workspace_namespace"):
            expected_workspace = str(uuid4())
            expected_namespace = str(uuid4())
            with mock.patch("terra_notebook_utils.workspace.get_workspace_namespace", return_value=expected_namespace):
                with ConfigOverride(str(uuid4()), str(uuid4())):
                    terra_notebook_utils.cli.WORKSPACE_GOOGLE_PROJECT = None
                    workspace, namespace = Config.resolve(expected_workspace, expected_namespace)
                    self.assertEqual(expected_workspace, workspace)
                    self.assertEqual(expected_namespace, namespace)

    def test_config_set(self):
        new_workspace = f"{uuid4()}"
        new_workspace_namespace = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            with ConfigOverride(None, None, tf.name):
                Config.write()
                args = argparse.Namespace(workspace=new_workspace)
                terra_notebook_utils.cli.config.set_config_workspace(args)
                args = argparse.Namespace(workspace_namespace=new_workspace_namespace)
                terra_notebook_utils.cli.config.set_config_workspace_namespace(args)
                with open(tf.name) as fh:
                    data = json.loads(fh.read())
                self.assertEqual(data, dict(workspace=new_workspace,
                                            workspace_namespace=new_workspace_namespace))

class TestTerraNotebookUtilsCLI_VCF(SuppressWarningsMixin, CLITestMixin, unittest.TestCase):
    common_kwargs = dict(workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
    vcf_drs_url = "drs://dg.4503/57f58130-2d66-4d46-9b2b-539f7e6c2080"

    @classmethod
    def setUpClass(cls):
        src_uri = ("gs://genomics-public-data/1000-genomes"
                   "/vcf/ALL.chr2.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf")
        cls.gs_uri = f"gs://{WORKSPACE_BUCKET}/test_vcf_cli/{uuid4()}"
        partial_bgzip_vcf = partialize_vcf(src_uri, 500)
        upload_data(cls.gs_uri, partial_bgzip_vcf)

    @testmode("workspace_access")
    def test_head(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path=self.gs_uri)

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_head_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path=self.vcf_drs_url)

    @testmode("workspace_access")
    def test_samples(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples, path=self.gs_uri)

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_samples_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples,
                           path=self.vcf_drs_url)

    def test_stats(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats, path=self.gs_uri)

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_stats_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats,
                           path=self.vcf_drs_url)


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Workspace(CLITestMixin, unittest.TestCase):
    def test_list(self):
        self._test_cmd(terra_notebook_utils.cli.workspace.list_workspaces)

    def test_get(self):
        self._test_cmd(terra_notebook_utils.cli.workspace.get_workspace,
                       workspace=WORKSPACE_NAME,
                       workspace_namespace="firecloud-cgl")


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Profile(CLITestMixin, unittest.TestCase):
    def list_workspace_namespaces(self):
        self._test_cmd(terra_notebook_utils.cli.profile.list_workspace_namespaces)


# These tests will only run on `make dev_env_access_test` command as they are testing DRS against Terra Dev env
@testmode("dev_env_access")
class TestTerraNotebookUtilsCLI_DRSInDev(CLITestMixin, unittest.TestCase):
    jade_dev_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-" \
                   "2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060"
    expected_crc32c = "/VKJIw=="

    def test_copy(self):
        with self.subTest("test copy to local path"):
            with NamedTemporaryFile() as tf:
                self._test_cmd(terra_notebook_utils.cli.drs.drs_copy,
                               drs_url=self.jade_dev_url,
                               dst=tf.name,
                               workspace=WORKSPACE_NAME,
                               workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test copy to gs bucket"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.drs.drs_copy,
                           drs_url=self.jade_dev_url,
                           dst=f"gs://{WORKSPACE_BUCKET}/{key}",
                           workspace=WORKSPACE_NAME,
                           workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
            out = io.BytesIO()
            blob.download_to_file(out)
            blob.reload()  # download_to_file causes the crc32c to change, for some reason. Reload blob to recover.
            self.assertEqual(self.expected_crc32c, blob.crc32c)
            self.assertEqual(_crc32c(out.getvalue()), blob.crc32c)


@testmode("controlled_access")
class TestTerraNotebookUtilsCLI_DRS(CLITestMixin, unittest.TestCase):
    drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"
    expected_crc32c = "LE1Syw=="

    def test_copy(self):
        with self.subTest("test local"):
            with NamedTemporaryFile() as tf:
                self._test_cmd(terra_notebook_utils.cli.drs.drs_copy,
                               drs_url=self.drs_url,
                               dst=tf.name,
                               workspace=WORKSPACE_NAME,
                               workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test gs"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.drs.drs_copy,
                           drs_url=self.drs_url,
                           dst=f"gs://{WORKSPACE_BUCKET}/{key}",
                           workspace=WORKSPACE_NAME,
                           workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
            out = io.BytesIO()
            blob.download_to_file(out)
            blob.reload()  # download_to_file causes the crc32c to change, for some reason. Reload blob to recover.
            self.assertEqual(self.expected_crc32c, blob.crc32c)
            self.assertEqual(_crc32c(out.getvalue()), blob.crc32c)

    def test_head(self):
        with self.subTest("Test heading a drs url."):
            cmd = [f'{pkg_root}/scripts/tnu', 'drs', 'head', self.drs_url,
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
            stdout = self._run_cmd(cmd)
            self.assertEqual(stdout, b'\x1f', stdout)
            self.assertEqual(len(stdout), 1, stdout)

            cmd = [f'{pkg_root}/scripts/tnu', 'drs', 'head', self.drs_url,
                   '--bytes=3',
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
            stdout = self._run_cmd(cmd)
            self.assertEqual(stdout, b'\x1f\x8b\x08')
            self.assertEqual(len(stdout), 3)

            for buffer in [1, 2, 10, 11]:
                cmd = [f'{pkg_root}/scripts/tnu', 'drs', 'head', self.drs_url,
                       '--bytes=10',
                       f'--buffer={buffer}',
                       f'--workspace={WORKSPACE_NAME} ',
                       f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
                stdout = self._run_cmd(cmd)
                self.assertEqual(stdout, b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03')
                self.assertEqual(len(stdout), 10)

        with self.subTest("Test heading a non-existent drs url."):
            fake_drs_url = 'drs://nothing/asf/f'
            cmd = [f'{pkg_root}/scripts/tnu', 'drs', 'head', fake_drs_url,
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
            with self.assertRaises(subprocess.CalledProcessError):
                self._run_cmd(cmd)

@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Table(CLITestMixin, unittest.TestCase):
    common_kwargs = dict(workspace=WORKSPACE_NAME, workspace_namespace=WORKSPACE_GOOGLE_PROJECT)

    @classmethod
    def setUpClass(cls):
        cls.table = "simple_germline_variation"
        cls.table_data = list()
        for row in table.list_rows(cls.table):
            e = row.attributes.copy()
            e['entity_id'] = row.name
            cls.table_data.append(e)
        cls.columns = list(row.attributes.keys())

    def setUp(self):
        self.row_index = randint(0, len(self.table_data) - 1)
        self.entity_id = self.table_data[self.row_index]['entity_id']
        self.column = self.columns[randint(0, len(self.columns) - 1)]
        self.cell_value = self.table_data[self.row_index][self.column]

    def test_list(self):
        self._test_cmd(terra_notebook_utils.cli.table.list_tables)

    def test_list_rows(self):
        self._test_cmd(terra_notebook_utils.cli.table.list_rows, table="simple_germline_variation")

    def test_get_row(self):
        out = self._test_cmd(terra_notebook_utils.cli.table.get_row,
                             table=self.table,
                             id=self.entity_id)
        row = json.loads(out)
        row['entity_id'] = row.pop(f"{self.table}_id")
        self.assertEqual(row, self.table_data[self.row_index])

def _crc32c(data: bytes) -> str:
    # Compute Google's wonky base64 encoded crc32c checksum
    return base64.b64encode(google_crc32c.Checksum(data).digest()).decode("utf-8")


if __name__ == '__main__':
    unittest.main()
