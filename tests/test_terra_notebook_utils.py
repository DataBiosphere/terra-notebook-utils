#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
import glob
import pytz
import tempfile
from uuid import uuid4
from random import randint
from datetime import datetime
from functools import lru_cache
from unittest import mock
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator

import gs_chunked_io as gscio

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import TestCaseSuppressWarnings, config
from tests.infra.testmode import testmode
from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME
from terra_notebook_utils import drs, table, gs, tar_gz, xprofile, progress, vcf, workspace


class TestTerraNotebookUtilsTable(TestCaseSuppressWarnings):
    @testmode("workspace_access")
    def test_fetch_attribute(self):
        table_name = "simple_germline_variation"
        filter_column = "name"
        filter_val = "74b42836-16ab-4bf7-a683-dc5e603d0bc7"
        key = "object_id"
        val = table.fetch_attribute(table_name, filter_column, filter_val, key)
        self.assertEqual(val, "drs://dg.4503/6e73a376-f7fd-47ed-ac99-0567bb5a5993")

    @testmode("workspace_access")
    def test_fetch_object_id(self):
        table_name = "simple_germline_variation"
        file_name = "NWD531899.freeze5.v1.vcf.gz"
        val = table.fetch_object_id(table_name, file_name)
        self.assertEqual(val, "drs://dg.4503/651a4ad1-06b5-4534-bb2c-1f8ed51134f6")

    @testmode("controlled_access")
    def test_get_access_token(self):
        gs.get_access_token()

    @testmode("workspace_access")
    def test_print_column(self):
        table_name = "simple_germline_variation"
        column = "file_name"
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


class TestTerraNotebookUtilsDRS(TestCaseSuppressWarnings):
    drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"

    @testmode("controlled_access")
    def test_resolve_drs_for_google_storage(self):
        _, info = drs.resolve_drs_for_gs_storage(self.drs_url)
        self.assertEqual(info.bucket_name, "topmed-irc-share")
        self.assertEqual(info.key, "genomes/NWD522743.b38.irc.v1.cram.crai")

    @testmode("controlled_access")
    def test_download(self):
        drs.copy_to_local(self.drs_url, "foo")

    @testmode("controlled_access")
    def test_oneshot_copy(self):
        drs.copy_to_bucket(self.drs_url, "test_oneshot_object")

    @testmode("controlled_access")
    def test_multipart_copy(self):
        with mock.patch("terra_notebook_utils.MULTIPART_THRESHOLD", 1024 * 1024):
            drs.copy_to_bucket(self.drs_url, "test_oneshot_object")

    @testmode("controlled_access")
    def test_copy(self):
        with self.subTest("Test copy to local location"):
            filepath = f"test_copy_object_{uuid4()}"
            drs.copy(self.drs_url, filepath)
        with self.subTest("Test copy to bucket location"):
            key = f"gs://{WORKSPACE_BUCKET}/test_oneshot_object_{uuid4()}"
            drs.copy(self.drs_url, key)

    @testmode("controlled_access")
    def test_copy_batch(self):
        drs_urls = {
            "NWD522743.b38.irc.v1.cram.crai": "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35",  # 1631686 bytes
            # "NWD961306.freeze5.v1.vcf.gz": "drs://dg.4503/6e73a376-f7fd-47ed-ac99-0567bb5a5993",  # 2679331445 bytes
            # "NWD531899.freeze5.v1.vcf.gz": "drs://dg.4503/651a4ad1-06b5-4534-bb2c-1f8ed51134f6",  # 2679411265 bytes
            "data_phs001237.v2.p1.c1.avro.gz": "drs://dg.4503/26e11149-5deb-4cd7-a475-16997a825655",  # 1115092 bytes
            "RootStudyConsentSet_phs001237.TOPMed_WGS_WHI.v2.p1.c1.HMB-IRB.tar.gz":
                "drs://dg.4503/e9c2caf2-b2a1-446d-92eb-8d5389e99ee3",  # 332237 bytes
        }
        pfx = f"test-batch-copy/{uuid4()}"
        bucket = gs.get_client().bucket(WORKSPACE_BUCKET)
        with self.subTest("gs bucket"):
            with mock.patch("terra_notebook_utils.drs.MULTIPART_THRESHOLD", 400000):
                drs.copy_batch(list(drs_urls.values()), f"gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10/{pfx}")
                for name in list(drs_urls.keys()):
                    blob = bucket.get_blob(f"{pfx}/{name}")
                    self.assertGreater(blob.size, 0)
        with self.subTest("local filesystem"):
            with tempfile.TemporaryDirectory() as dirname:
                drs.copy_batch(list(drs_urls.values()), dirname)
                names = [os.path.basename(path) for path in _list_tree(dirname)]
                self.assertEqual(sorted(names), sorted(list(drs_urls.keys())))

    # Probably don't want to run this test very often. Once a week?
    def _test_extract_tar_gz(self):
        drs_url = "drs://dg.4503/273f3453-4d16-4ddd-8877-dbac958a4f4d"  # Amish cohort v4 VCF  # no access
        drs_url = "drs://dg.4503/88f9acc7-11f1-4478-b407-725d2dfab43d"  # cohort VCF tarball # no access
        drs_url = "drs://dg.4503/51a10328-c6b9-49f7-8fd7-94eaf193210f"  # cohort VCF tarball # no access
        drs_url = "drs://dg.4503/954acb05-fdc7-4fef-ad4e-9eb1a85117c4"  # .tar (not .tar.gz) # no access
        drs_url = "drs://dg.4503/da8cb525-4532-4d0f-90a3-4d327817ec73"  # cohort VCF tarball
        drs_url = "drs://dg.4503/954acb05-fdc7-4fef-ad4e-9eb1a85117c4"  # .tar (not .tar.gz) # no access
        drs_url = "drs://dg.4503/ada7d89d-a739-4572-83ec-7cf268baa1bf"  # .tar (not .tar.gz) # no access
        drs_url = "drs://dg.4503/828d82a1-e6cd-4a24-a593-f7e8025c7d71"  # .tar (not .tar.gz)
        drs.extract_tar_gz(drs_url, "test_cohort_extract_{uuid4()}")

    @testmode("workspace_access")
    def test_arg_propagation(self):
        resp_json = mock.MagicMock(return_value={
            'googleServiceAccount': {'data': {'project_id': "foo"}},
            'dos': {'data_object': {'urls': [{'url': 'gs://asdf/asdf'}]}}
        })
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        from contextlib import ExitStack
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.copy"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.gscio"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.tar_gz"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.requests", post=requests_post))
            with mock.patch("terra_notebook_utils.drs.enable_requester_pays") as enable_requester_pays:
                with self.subTest("Copy to local"):
                    drs.copy(self.drs_url, "some_bucketsome_key")
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)
                with self.subTest("Copy to bucket"):
                    enable_requester_pays.reset_mock()
                    drs.copy(self.drs_url, "gs://some_bucket/some_key")
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)
                with self.subTest("Extract tarball"):
                    enable_requester_pays.reset_mock()
                    drs.extract_tar_gz(self.drs_url, "some_pfx", "some_bucket")
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)

    def test_url_basename(self):
        with self.subTest("Should raise for invalid or missing schemas"):
            for broken_url in ["alsdf", "s:/asdf", "s//saldf"]:
                with self.assertRaises(ValueError):
                    drs._url_basename(broken_url)
        with self.subTest("Should raise when basename is absent"):
            with self.assertRaises(ValueError):
                drs._url_basename("drs://alskdjflasjdf")
        with self.subTest("Should return drs url basename"):
            expected_basename = "my_cool_basename"
            basename = drs._url_basename(f"drs://asldkfj/argle/{expected_basename}")
            self.assertEqual(expected_basename, basename)

@testmode("workspace_access")
class TestTerraNotebookUtilsTARGZ(TestCaseSuppressWarnings):
    def test_extract(self):
        with self.subTest("Test tarball extraction to local filesystem"):
            with open("tests/fixtures/test_archive.tar.gz", "rb") as fh:
                tar_gz.extract(fh, root="untar_test")
            for filename in glob.glob("tests/fixtures/test_archive/*"):
                with open(filename) as a:
                    with open(os.path.join("untar_test/test_archive", os.path.basename(filename))) as b:
                        self.assertEqual(a.read(), b.read())
        with self.subTest("Test tarball extraction to GS bucket"):
            start_time = time.time()
            client = gs.get_client()
            bucket = client.bucket(os.environ['WORKSPACE_BUCKET'][5:])
            with open("tests/fixtures/test_archive.tar.gz", "rb") as fh:
                tar_gz.extract(fh, bucket, root="untar_test")
            for filename in glob.glob("tests/fixtures/test_archive/*"):
                key = f"untar_test/test_archive/{os.path.basename(filename)}"
                blob = bucket.get_blob(key)
                self.assertIsNotNone(blob)
                age = (datetime.now(pytz.utc) - blob.time_created).total_seconds()
                self.assertGreater(time.time() - start_time, age)


@testmode("workspace_access")
class TestTerraNotebookUtilsVCF(TestCaseSuppressWarnings):
    def test_vcf_info(self):
        key = "consent1/HVH_phs000993_TOPMed_WGS_freeze.8.chr7.hg38.vcf.gz"
        blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
        vcf_info = vcf.VCFInfo.with_blob(blob)
        self.assertEqual("chr7", vcf_info.chrom)
        self.assertEqual("10007", vcf_info.pos)

    def test_vcf_info_non_block_gzipped(self):
        path = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(path, "fixtures", "non_block_gzipped.vcf.gz")
        vcf_info = vcf.VCFInfo.with_file(path)
        self.assertEqual("chr7", vcf_info.chrom)
        self.assertEqual("10007", vcf_info.pos)

    def test_prepare_merge_workflow_input(self):
        prefixes = ["consent1",
                    "phg001280.v1.TOPMed_WGS_Amish_v4.genotype-calls-vcf.WGS_markerset_grc38.c2.HMB-IRB-MDS"]

        fmt_1 = f"gs://{WORKSPACE_BUCKET}/{prefixes[0]}/HVH_phs000993_TOPMed_WGS_freeze.8.{{chrom}}.hg38.vcf.gz"
        fmt_2 = f"gs://{WORKSPACE_BUCKET}/{prefixes[1]}/Amish_phs000956_TOPMed_WGS_freeze.8.{{chrom}}.hg38.vcf.gz"
        expected_input_keys = sorted([",".join([fmt_1.format(chrom=c), fmt_2.format(chrom=c)])
                                      for c in vcf.VCFInfo.chromosomes if c != "chrY"])
        expected_output_keys = sorted([f"gs://{WORKSPACE_BUCKET}/merged/{c}.vcf.bgz"
                                       for c in vcf.VCFInfo.chromosomes if c != "chrY"])

        table_name = f"test_merge_input_{uuid4()}"
        vcf.prepare_merge_workflow_input(table_name, prefixes, "merged")
        ents = table.list_entities(table_name)
        input_keys = list()
        output_keys = list()
        for e in ents:
            input_keys.append(e['attributes']['inputs'])
            output_keys.append(e['attributes']['output'])
        self.assertEqual(expected_input_keys, sorted(input_keys))
        self.assertEqual(expected_output_keys, sorted(output_keys))


@testmode("workspace_access")
class TestTerraNotebookUtilsProgress(TestCaseSuppressWarnings):
    def test_progress_reporter(self):
        with progress.ProgressReporter() as pr:
            pr.checkpoint(2)
            pr.checkpoint(5)
            pr.checkpoint(7)
            time.sleep(2)
            self.assertEqual(14, pr.units_processed)

    def test_rate_limited(self):
        rate = 50
        with self.subTest("Should be rate limited to expected number of calls"):
            call_info = dict(number_of_calls=0)

            @progress.RateLimited(rate)
            def rate_limited_func():
                call_info['number_of_calls'] += 1

            start_time = time.time()
            duration = 0
            while duration <= 4.5 * 1 / rate:
                rate_limited_func()
                duration = time.time() - start_time
            self.assertEqual(5, call_info['number_of_calls'])

        with self.subTest("Should get expected exception for calling too quickly"):
            class MockRateLimitedException(Exception):
                pass

            @progress.RateLimited(rate, MockRateLimitedException)
            def raising_rate_limited_func():
                pass

            with self.assertRaises(MockRateLimitedException):
                for _ in range(2):
                    raising_rate_limited_func()

        with self.subTest("Should be able to avoid rate limit exceptions using reset"):
            for _ in range(2):
                raising_rate_limited_func.reset()
                raising_rate_limited_func()


@testmode("workspace_access")
class TestTerraNotebookUtilsGS(TestCaseSuppressWarnings):
    def test_list_bucket(self):
        for key in gs.list_bucket("consent1"):
            print(key)


@testmode("workspace_access")
class TestTerraNotebookUtilsWorkspace(TestCaseSuppressWarnings):
    namespace = "firecloud-cgl"

    def test_get_workspace(self):
        with self.subTest("Test get workspace with namespace"):
            ws = workspace.get_workspace(WORKSPACE_NAME, self.namespace)
            self.assertEqual(ws['workspace']['name'], WORKSPACE_NAME)
            self.assertEqual(ws['workspace']['namespace'], self.namespace)
        with self.subTest("Test get workspace without namespace"):
            ws = workspace.get_workspace(WORKSPACE_NAME)
            self.assertEqual(ws['workspace']['name'], WORKSPACE_NAME)
            self.assertEqual(ws['workspace']['namespace'], self.namespace)
        with self.subTest("Test get workspace without workspace name and namespace"):
            ws = workspace.get_workspace()
            self.assertEqual(ws['workspace']['name'], WORKSPACE_NAME)
            self.assertEqual(ws['workspace']['namespace'], self.namespace)

    def test_list_workspaces(self):
        ws_list = workspace.list_workspaces()
        names = [ws['workspace']['name'] for ws in ws_list]
        self.assertIn(WORKSPACE_NAME, names)

    def test_get_workspace_bucket(self):
        with self.subTest("Get workspace bucket without workspace name"):
            bucket = workspace.get_workspace_bucket()
            self.assertEqual(WORKSPACE_BUCKET, bucket)
        with self.subTest("Get workspace bucket with workspace name"):
            bucket = workspace.get_workspace_bucket(WORKSPACE_NAME)
            self.assertEqual(WORKSPACE_BUCKET, bucket)
        with self.subTest("Bogus workspace should return None"):
            bucket = workspace.get_workspace_bucket(f"{uuid4()}")
            self.assertIsNone(bucket)

    def test_get_workspace_namespace(self):
        with self.subTest("Get workspace namespace without workspace name"):
            namespace = workspace.get_workspace_namespace()
            self.assertEqual(self.namespace, namespace)
        with self.subTest("Get workspace namespace with workspace name"):
            namespace = workspace.get_workspace_namespace(WORKSPACE_NAME)
            self.assertEqual(self.namespace, namespace)
        with self.subTest("Bogus workspace should return None"):
            namespace = workspace.get_workspace_namespace(f"{uuid4()}")
            self.assertIsNone(namespace)

    def remove_workflow_logs(self):
        bucket = gs.get_client().bucket(WORKSPACE_BUCKET)
        submission_ids = [f"{uuid4()}", f"{uuid4()}"]
        manifests = [self._upload_workflow_logs(bucket, submission_id)
                     for submission_id in submission_ids]
        for submission_id, manifest in zip(submission_ids, manifests):
            deleted_manifest = workspace.remove_workflow_logs(submission_id=submission_id)
            self.assertEqual(sorted(manifest), sorted(deleted_manifest))

    def _upload_workflow_logs(self, bucket, submission_id):
        bucket = gs.get_client().bucket(WORKSPACE_BUCKET)
        with open("tests/fixtures/workflow_logs_manifest.txt") as fh:
            manifest = [line.strip().format(submission_id=submission_id) for line in fh]
        with ThreadPoolExecutor(max_workers=8) as e:
            futures = [e.submit(bucket.blob(key).upload_from_file, io.BytesIO(b""))
                       for key in manifest]
            for f in as_completed(futures):
                f.result()
        return manifest

def _list_tree(root) -> Generator[str, None, None]:
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)


if __name__ == '__main__':
    unittest.main()
