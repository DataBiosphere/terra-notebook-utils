#!/usr/bin/env python
import os
import sys
import unittest
import tempfile
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from tests import CLITestMixin
from tests.infra import SuppressWarningsMixin, upload_data
from tests.infra.partialize_vcf import partialize_vcf

from terra_notebook_utils import vcf, drs, WORKSPACE_BUCKET, WORKSPACE_GOOGLE_PROJECT
import terra_notebook_utils.cli.commands.vcf


class TestTerraNotebookUtilsVCF(SuppressWarningsMixin, unittest.TestCase):
    @testmode("controlled_access")
    def test_vcf_info_drs(self):
        drs_uri = "drs://dg.4503/6aad51c2-2ea9-4248-8a38-7d52c10cfc89"
        info = vcf.VCFInfo.with_blob(drs.blob_for_url(drs_uri))
        self.assertEqual("chr5", info.chrom)
        self.assertEqual("10018", info.pos)

    @testmode("workspace_access")
    def test_vcf_info(self):
        src_uri = ("gs://genomics-public-data/1000-genomes"
                   "/vcf/ALL.chr2.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf")
        key = f"test_vcf_info/{uuid4()}"
        partial_bgzip_vcf = partialize_vcf(src_uri, 500)
        partial_gzip_vcf = partialize_vcf(src_uri, 500, zip_format="gzip")
        with tempfile.TemporaryDirectory() as tempdir:
            tests = [("gs", f"gs://{WORKSPACE_BUCKET}/{key}", partial_bgzip_vcf),
                     ("local", os.path.join(tempdir, f"{uuid4()}"), partial_bgzip_vcf),
                     ("not block gzipped", os.path.join(tempdir, f"{uuid4()}"), partial_gzip_vcf)]
            for test_name, uri, data in tests:
                with self.subTest(test_name):
                    upload_data(uri, data)
                    info = vcf.VCFInfo.with_blob(drs.blob_for_url(uri))
                    self.assertEqual("2", info.chrom)
                    self.assertEqual("10133", info.pos)

class TestTerraNotebookUtilsCLI_VCF(SuppressWarningsMixin, CLITestMixin, unittest.TestCase):
    common_kwargs = dict(billing_project=WORKSPACE_GOOGLE_PROJECT)
    vcf_drs_url = "drs://dg.4503/57f58130-2d66-4d46-9b2b-539f7e6c2080"

    @classmethod
    def setUpClass(cls):
        src_uri = ("gs://genomics-public-data/1000-genomes"
                   "/vcf/ALL.chr2.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf")
        cls.gs_uri = f"gs://{WORKSPACE_BUCKET}/test_vcf_cli/{uuid4()}"
        partial_bgzip_vcf = partialize_vcf(src_uri, 500)
        upload_data(cls.gs_uri, partial_bgzip_vcf)

    @testmode("workspace_access")
    def test_head_vcf(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.head, path=self.gs_uri)

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.head, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_head_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.head, path=self.vcf_drs_url)

    @testmode("workspace_access")
    def test_samples(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.samples, path=self.gs_uri)

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.samples,
                           path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_samples_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.samples,
                           path=self.vcf_drs_url)

    def test_stats(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.stats, path=self.gs_uri)

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.stats, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode("controlled_access")
    def test_stats_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.commands.vcf.stats,
                           path=self.vcf_drs_url)

if __name__ == '__main__':
    unittest.main()
