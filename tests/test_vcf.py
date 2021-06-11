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
from tests.infra import SuppressWarningsMixin, upload_data
from tests.infra.partialize_vcf import partialize_vcf

from terra_notebook_utils import vcf, drs, WORKSPACE_BUCKET


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

if __name__ == '__main__':
    unittest.main()
