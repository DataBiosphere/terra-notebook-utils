#!/usr/bin/env python
import io
import os
import sys
import unittest
import tempfile
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import WORKSPACE_BUCKET
from terra_notebook_utils import drs, gs, vcf
from tests.infra import SuppressWarningsMixin, upload_data
from tests.infra.partialize_vcf import partialize_vcf


class TestTerraNotebookUtilsVCF(SuppressWarningsMixin, unittest.TestCase):
    @testmode("controlled_access")
    def test_vcf_info_drs(self):
        drs_uri = "drs://dg.4503/6aad51c2-2ea9-4248-8a38-7d52c10cfc89"
        vcf_info = vcf.vcf_info(drs_uri)
        self.assertEqual("chr5", vcf_info.chrom)
        self.assertEqual("10018", vcf_info.pos)

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
                    vcf_info = vcf.vcf_info(uri)
                    self.assertEqual("2", vcf_info.chrom)
                    self.assertEqual("10133", vcf_info.pos)

if __name__ == '__main__':
    unittest.main()
