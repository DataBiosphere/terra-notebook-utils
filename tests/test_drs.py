#!/usr/bin/env python
import io
import os
import sys
import base64
import unittest
import tempfile
import subprocess
import requests

from uuid import uuid4
from unittest import mock
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator, Optional

import jsonschema
import google_crc32c

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests import CLITestMixin
from tests.infra import SuppressWarningsMixin, get_env
from tests.infra.testmode import testmode
from terra_notebook_utils import drs, gs, WORKSPACE_BUCKET, WORKSPACE_NAME, WORKSPACE_NAMESPACE
import terra_notebook_utils.cli.commands.drs


TNU_TEST_GS_BUCKET = get_env("TNU_BLOBSTORE_TEST_GS_BUCKET")

DRS_URI_500_KB = "drs://dg.4503/5ec0e501-432e-4cad-808d-1a4e9100b7de"  # 1000 Genomes, 500.15 KB
DRS_URI_370_KB = "drs://dg.4503/6ffc2f59-2596-405c-befd-9634dc0ed837"  # 1000 Genomes, 370.38 KB
DRS_URI_003_MB = "drs://dg.4503/0f26beeb-d468-405e-abb7-412eb7bf8b19"  # 1000 Genomes, 2.5 MB

# These tests will only run on `make dev_env_access_test` command as they are testing DRS against Terra Dev env
@testmode("dev_env_access")
class TestTerraNotebookUtilsDRSInDev(SuppressWarningsMixin, unittest.TestCase):
    jade_dev_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-" \
                   "2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060"

    def test_resolve_drs_for_google_storage(self):
        info = drs.get_drs_info(self.jade_dev_url)
        self.assertEqual(info.bucket_name, "broad-jade-dev-data-bucket")
        self.assertEqual(info.key, "ca8edd48-e954-4c20-b911-b017fedffb67/c0e40912-8b14-43f6-9a2f-b278144d0060")
        self.assertEqual(info.name, "hapmap_3.3.hg38.vcf.gz")
        self.assertEqual(info.size, 62043448)

    def test_head(self):
        the_bytes = drs.head(self.jade_dev_url)
        self.assertEqual(1, len(the_bytes))

        with self.assertRaises(drs.GSBlobInaccessible):
            fake_drs_url = 'drs://nothing'
            drs.head(fake_drs_url)

    def test_copy_to_local(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy_to_local(self.jade_dev_url, tf.name)

    def test_download(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy(self.jade_dev_url, tf.name)

    def test_copy_to_bucket(self):
        key = f"gs://{WORKSPACE_BUCKET}/test_oneshot_object_{uuid4()}"
        drs.copy_to_bucket(self.jade_dev_url, key)


class TestTerraNotebookUtilsDRS(SuppressWarningsMixin, unittest.TestCase):
    # BDC data on Google
    # DRS response contains GS native url + credentials + signed URL if requested
    drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"
    # CRDC PDC data on AWS (only)
    # DRS response contains signed URL if requested
    drs_url_signed = "drs://dg.4DFC:dg.4DFC/00040a6f-b7e5-4e5c-ab57-ee92a0ba8201"
    # DRS response contains GS native url + credentials (no signed URL)
    drs_url_requester_pays = "drs://dg.ANV0/1b1ee6fc-6560-4b08-9c44-36d46bf4daf1"
    jade_dev_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-2a2bfaa24c7a_" \
                   "c0e40912-8b14-43f6-9a2f-b278144d0060"

    # drs_resolver responses
    mock_jdr_response = {
        'contentType': 'application/octet-stream',
        'size': 15601108255,
        'timeCreated': '2020-04-27T15:56:09.696Z',
        'timeUpdated': '2020-04-27T15:56:09.696Z',
        'bucket': 'broad-jade-dev-data-bucket',
        'name': 'fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
        'fileName': 'HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam',
        'gsUri':
            'gs://broad-jade-dev-data-bucket/fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
        'googleServiceAccount': None,
        'hashes': {
            'md5': '336ea55913bc261b72875bd259753046',
            'sha256': 'f76877f8e86ec3932fd2ae04239fbabb8c90199dab0019ae55fa42b31c314c44',
            'crc32c': '8a366443'
        }
    }
    mock_drs_resolver_response_missing_fields = {
        'contentType': 'application/octet-stream',
        'bucket': 'broad-jade-dev-data-bucket',
        'name': 'fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
        'gsUri':
            'gs://broad-jade-dev-data-bucket/fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
        'googleServiceAccount': {
            'data': {
                'project_id': "foo"
            }
        },
        'hashes': {
            'md5': '336ea55913bc261b72875bd259753046',
        }
    }
    mock_drs_resolver_response_without_gs_uri = {
        'contentType': 'application/octet-stream',
        'bucket': 'broad-jade-dev-data-bucket',
        'googleServiceAccount': {
            'data': {
                'project_id': "foo"
            }
        },
        'hashes': {
            'md5': '336ea55913bc261b72875bd259753046',
        }
    }
    mock_drs_resolver_error_response = {
        "status": 500,
        "response": {
            "req": {
                "method": "GET",
                "url": "https://jade.datarepo-dev.broadinstitute.org/ga4gh/drs/v1/objects/drs-path",
                "headers": {
                    "user-agent": "node-superagent/3.8.3",
                    "authori - not gonna get me this time, git-secrets": "A bear with a token"
                }
            },
            "header": {
                "date": "Wed, 09 Sep 2020 14:52:10 GMT",
                "server": "nginx/1.18.0",
                "x-frame-options": "SAMEORIGIN",
                "content-type": "application/json;charset=UTF-8",
                "transfer-encoding": "chunked",
                "via": "1.1 google",
                "alt-svc": "clear",
                "connection": "close"
            },
            "status": 500,
            "text": "{\"msg\":\"User 'null' does not have required action: read_data\",\"status_code\":500}"
        }
    }
    mock_drs_resolver_empty_error_response = {
        "status": 500,
        "response": {
            "status": 500,
        }
    }

    def test_resolve_targets(self):
        expected_name = f"{uuid4()}"
        for name in (expected_name, None):
            info = drs.DRSInfo(credentials=None,
                               access_url=None,
                               checksums=None,
                               bucket_name=None,
                               key=f"/foo/bar/{expected_name}",
                               name=name,
                               size=None,
                               updated=None)
            local_tests = [
                (os.path.join(os.getcwd(), expected_name), "."),
                (os.path.join(os.path.sep, expected_name), "/"),
                (os.path.join(os.getcwd(), "foo", expected_name), "foo/"),
                (os.path.join(os.getcwd(), "foo", expected_name), "foo//"),
                ("/foo", "/foo"),
                ("/foo/bar", "/foo/bar"),
                (os.path.join(os.getcwd(), "foo", "bar", expected_name), "foo/bar/"),
            ]

            target_bucket_name = f"{uuid4()}"
            bucket_tests = [
                ((target_bucket_name, expected_name), f"gs://{target_bucket_name}"),
                ((target_bucket_name, expected_name), f"gs://{target_bucket_name}/"),
                ((target_bucket_name, "foo"), f"gs://{target_bucket_name}/foo"),
                ((target_bucket_name, f"foo/{expected_name}"), f"gs://{target_bucket_name}/foo/"),
                ((target_bucket_name, f"foo//{expected_name}"), f"gs://{target_bucket_name}/foo//"),
                ((target_bucket_name, "foo/bar"), f"gs://{target_bucket_name}/foo/bar"),
                ((target_bucket_name, f"foo/bar/{expected_name}"), f"gs://{target_bucket_name}/foo/bar/"),
                ((target_bucket_name, f"foo/bar//{expected_name}"), f"gs://{target_bucket_name}/foo/bar//"),
            ]

            with self.subTest("local"):
                for expected, dst in local_tests:
                    self.assertEqual(expected, drs._resolve_local_target(dst, info))

            with self.subTest("bucket"):
                for expected, dst in bucket_tests:
                    self.assertEqual(expected, drs._resolve_bucket_target(dst, info))

    @testmode("controlled_access")
    def test_resolve_drs_for_google_storage(self):
        info = drs.get_drs_info(self.drs_url)
        self.assertEqual(info.bucket_name, "topmed-irc-share")
        self.assertEqual(info.key, "genomes/NWD522743.b38.irc.v1.cram.crai")

    @testmode("controlled_access")
    def test_head(self):
        the_bytes = drs.head(DRS_URI_370_KB)
        self.assertEqual(1, len(the_bytes))

        the_bytes = drs.head(DRS_URI_370_KB, num_bytes=10)
        self.assertEqual(10, len(the_bytes))

        with self.assertRaises(drs.BlobNotFoundError):
            fake_drs_url = 'drs://nothing'
            drs.head(fake_drs_url)

    @testmode("controlled_access")
    def test_download(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy(self.drs_url, tf.name)

    @testmode("controlled_access")
    def test_copy(self):
        for drs_url in [self.drs_url, self.drs_url_signed]:
            with self.subTest("Test copy to local location"):
                with tempfile.NamedTemporaryFile() as tf:
                    drs.copy(drs_url, tf.name)
                    self.assertTrue(os.path.isfile(tf.name))
            with self.subTest("Test copy to bucket location"):
                key = f"test_oneshot_object_{uuid4()}"
                drs.copy(drs_url, f"gs://{TNU_TEST_GS_BUCKET}/{key}")
                self.assertTrue(self._gs_obj_exists(key))
            with self.subTest("Test copy to bare bucket"):
                name = drs.info(drs_url)['name']
                drs.copy(drs_url, f"gs://{TNU_TEST_GS_BUCKET}")
                self.assertTrue(self._gs_obj_exists(name))

    def _gs_obj_exists(self, key: str) -> bool:
        return gs.get_client().bucket(TNU_TEST_GS_BUCKET).blob(key).exists()

    @testmode("controlled_access")
    def test_copy_batch(self):
        drs_urls = {
            # 1631686 bytes # name property disapeard from DRS response :(
            # "NWD522743.b38.irc.v1.cram.crai": "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35",  # 1631686 bytes
            "NWD522743.b38.irc.v1.cram.crai": "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35",
            "data_phs001237.v2.p1.c1.avro.gz": "drs://dg.4503/26e11149-5deb-4cd7-a475-16997a825655",  # 1115092 bytes
            "RootStudyConsentSet_phs001237.TOPMed_WGS_WHI.v2.p1.c1.HMB-IRB.tar.gz":
                "drs://dg.4503/e9c2caf2-b2a1-446d-92eb-8d5389e99ee3",  # 332237 bytes

            # "NWD961306.freeze5.v1.vcf.gz": "drs://dg.4503/6e73a376-f7fd-47ed-ac99-0567bb5a5993",  # 2679331445 bytes
            # "NWD531899.freeze5.v1.vcf.gz": "drs://dg.4503/651a4ad1-06b5-4534-bb2c-1f8ed51134f6",  # 2679411265 bytes
        }
        pfx = f"test-batch-copy/{uuid4()}"
        bucket = gs.get_client().bucket(WORKSPACE_BUCKET)
        with self.subTest("gs bucket"):
            drs.copy_batch(list(drs_urls.values()), f"gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10/{pfx}")
            for name in list(drs_urls.keys()):
                blob = bucket.get_blob(f"{pfx}/{name}")
                self.assertGreater(blob.size, 0)
        with self.subTest("local filesystem"):
            with tempfile.TemporaryDirectory() as dirname:
                drs.copy_batch(list(drs_urls.values()), dirname)
                names = [os.path.basename(path) for path in _list_tree(dirname)]
                self.assertEqual(sorted(names), sorted(list(drs_urls.keys())))

    @testmode("controlled_access")
    def test_copy_batch_manifest(self):
        drs_uris = {
            "CCDG_13607_B01_GRM_WGS_2019-02-19_chr2.recalibrated_variants.annotated.clinical.txt": DRS_URI_003_MB,
            "CCDG_13607_B01_GRM_WGS_2019-02-19_chr9.recalibrated_variants.annotated.clinical.txt": DRS_URI_370_KB,
            "CCDG_13607_B01_GRM_WGS_2019-02-19_chr3.recalibrated_variants.annotated.clinical.txt": DRS_URI_500_KB,
        }
        named_drs_uris = {
            f"{uuid4()}": DRS_URI_003_MB,
            f"{uuid4()}": DRS_URI_370_KB,
            f"{uuid4()}": DRS_URI_500_KB,
        }
        pfx = f"test-batch-copy/{uuid4()}"
        bucket = gs.get_client().bucket(TNU_TEST_GS_BUCKET)
        with tempfile.TemporaryDirectory() as dirname:
            # create a mixed manifest with local and cloud destinations
            manifest = [dict(drs_uri=uri, dst=f"gs://{os.environ['TNU_BLOBSTORE_TEST_GS_BUCKET']}/{pfx}/")
                        for uri in drs_uris.values()]
            manifest.extend([dict(drs_uri=uri, dst=f"gs://{os.environ['TNU_BLOBSTORE_TEST_GS_BUCKET']}/{pfx}/{name}")
                             for name, uri in named_drs_uris.items()])
            manifest.extend([dict(drs_uri=uri, dst=dirname) for uri in drs_uris.values()])
            manifest.extend([dict(drs_uri=uri, dst=os.path.join(dirname, name))
                             for name, uri in named_drs_uris.items()])
            drs.copy_batch_manifest(manifest)
            for name in dict(**drs_uris, **named_drs_uris):
                blob = bucket.get_blob(f"{pfx}/{name}")
                self.assertGreater(blob.size, 0)
            names = [os.path.basename(path) for path in _list_tree(dirname)]
            self.assertEqual(sorted(names), sorted(list(dict(**drs_uris, **named_drs_uris).keys())))

        with self.subTest("malformed manifest"):
            manifest = [dict(a="b"), dict(drs_uri="drs://foo", dst=".")]
            with self.assertRaises(jsonschema.exceptions.ValidationError):
                drs.copy_batch_manifest(manifest)

    @testmode("controlled_access")
    def test_extract_tar_gz(self):
        expected_data = (b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00\x90 \xed]kO\\I\x92\xfd\xcc\xfc"
                         b"\x8a\xd2\xb4V\xfb2\xd7\xf9~\xac\x97\x910\xb6i$\x1b\xbb\r\xdb=\xd3_\x10\x862\xae\x1d\x0c"
                         b"\x0cU\xb8\xa7G\xfe\xf1{\xe2\xc6\xc9\xa2\xc0\xb8\xdb\xdd\xf2,_\xd2R\xc8\x87")
        # This test uses a hack property, `_extract_single_chunk`, to extract a small amount
        # of data from the cohort vcf pointed to by `drs://dg.4503/da8cb525-4532-4d0f-90a3-4d327817ec73`.
        with mock.patch("terra_notebook_utils.tar_gz._extract_single_chunk", True):
            drs_url = "drs://dg.4503/da8cb525-4532-4d0f-90a3-4d327817ec73"  # cohort VCF tarball
            pfx = "test_cohort_extract_{uuid4()}"
            drs.extract_tar_gz(drs_url, f"gs://{WORKSPACE_BUCKET}/{pfx}")
            for key in list_bucket(pfx):
                blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
                data = blob.download_as_bytes()[:len(expected_data)]
                self.assertEqual(data, expected_data)

    @testmode("workspace_access")
    def test_is_requester_pays(self):
        self.assertFalse(drs.is_requester_pays([self.drs_url]))
        self.assertFalse(drs.is_requester_pays([self.drs_url_signed]))
        self.assertTrue(drs.is_requester_pays([self.drs_url_requester_pays]))
        self.assertTrue(drs.is_requester_pays(
            [self.drs_url, self.drs_url_requester_pays, self.drs_url_signed]))

    @testmode("workspace_access")
    def test_arg_propagation_and_enable_requester_pays(self):
        resp_json = mock.MagicMock(return_value={
            'googleServiceAccount': {'data': {'project_id': "foo"}},
            'dos': {'data_object': {'urls': [{'url': 'gs://asdf/asdf'}]}}
        })
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.tar_gz"))
            es.enter_context(mock.patch("terra_notebook_utils.blobstore.gs.GSBlob.download"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.DRSCopyClient"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.GSBlob.open"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with mock.patch("terra_notebook_utils.drs.enable_requester_pays") as enable_requester_pays:
                with self.subTest("Access URL"):
                    try:
                        drs.access(self.drs_url_requester_pays)
                    except Exception:
                        pass  # Ignore downstream error due to complexity of mocking
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)
                with self.subTest("Copy to local"):
                    enable_requester_pays.reset_mock()
                    with tempfile.NamedTemporaryFile() as tf:
                        drs.copy(self.drs_url_requester_pays, tf.name)
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)
                with self.subTest("Copy to bucket"):
                    enable_requester_pays.reset_mock()
                    drs.copy(self.drs_url_requester_pays, "gs://some_bucket/some_key")
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)
                with self.subTest("Copy batch urls"):
                    enable_requester_pays.reset_mock()
                    with tempfile.TemporaryDirectory() as td_name:
                        drs.copy_batch_urls([self.drs_url, self.drs_url_requester_pays], td_name)
                        enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)
                with self.subTest("Copy batch manifest"):
                    enable_requester_pays.reset_mock()
                    with tempfile.TemporaryDirectory() as td_name:
                        manifest = [{"drs_uri": self.drs_url, "dst": td_name},
                                    {"drs_uri": self.drs_url_requester_pays, "dst": td_name}]
                        drs.copy_batch_manifest(manifest)
                        enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)
                with self.subTest("Extract tarball"):
                    enable_requester_pays.reset_mock()
                    drs.extract_tar_gz(self.drs_url_requester_pays)
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)
                with self.subTest("Head"):
                    enable_requester_pays.reset_mock()
                    drs.head(self.drs_url_requester_pays)
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_NAMESPACE)

    @testmode("workspace_access")
    def test_enable_requester_pays_not_called_when_not_necessary(self):
        resp_json = mock.MagicMock(return_value={
            'googleServiceAccount': {'data': {'project_id': "foo"}},
            'dos': {'data_object': {'urls': [{'url': 'gs://asdf/asdf'}]}}
        })
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.tar_gz"))
            es.enter_context(mock.patch("terra_notebook_utils.blobstore.gs.GSBlob.download"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.DRSCopyClient"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.GSBlob.open"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with mock.patch("terra_notebook_utils.drs.enable_requester_pays") as enable_requester_pays:
                with self.subTest("Access URL"):
                    try:
                        drs.access(self.drs_url)
                    except Exception:
                        pass  # Ignore downstream error due to complexity of mocking
                    enable_requester_pays.assert_not_called()
                with self.subTest("Copy to local"):
                    enable_requester_pays.reset_mock()
                    with tempfile.NamedTemporaryFile() as tf:
                        drs.copy(self.drs_url, tf.name)
                    enable_requester_pays.assert_not_called()
                with self.subTest("Copy to bucket"):
                    enable_requester_pays.reset_mock()
                    drs.copy(self.drs_url, "gs://some_bucket/some_key")
                    enable_requester_pays.assert_not_called()
                with self.subTest("Copy batch urls"):
                    enable_requester_pays.reset_mock()
                    with tempfile.TemporaryDirectory() as td_name:
                        drs.copy_batch_urls([self.drs_url, self.drs_url], td_name)
                        enable_requester_pays.assert_not_called()
                with self.subTest("Copy batch manifest"):
                    enable_requester_pays.reset_mock()
                    with tempfile.TemporaryDirectory() as td_name:
                        manifest = [{"drs_uri": self.drs_url, "dst": td_name},
                                    {"drs_uri": self.drs_url, "dst": td_name}]
                        drs.copy_batch_manifest(manifest)
                        enable_requester_pays.assert_not_called()
                with self.subTest("Extract tarball"):
                    enable_requester_pays.reset_mock()
                    drs.extract_tar_gz(self.drs_url)
                    enable_requester_pays.assert_not_called()
                with self.subTest("Head"):
                    enable_requester_pays.reset_mock()
                    drs.head(self.drs_url)
                    enable_requester_pays.assert_not_called()

    # test for when we get everything what we wanted in drs_resolver response
    def test_drs_resolver_response(self):
        fields = ["fileName",
                  "hashes",
                  "size",
                  "gsUri",
                  "bucket",
                  "name",
                  "timeUpdated",
                  "googleServiceAccount"]
        resp_json = mock.MagicMock(return_value=self.mock_jdr_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            actual_info = drs.get_drs_info(self.jade_dev_url)

            args, kwargs = requests_post.call_args
            self.assertEqual(kwargs['headers'].get('X-App-ID'), 'terra_notebook_utils')
            expected_json_body = dict(url=self.jade_dev_url, cloudPlatform="gs", fields=fields)
            self.assertEqual(kwargs['json'], expected_json_body)

            self.assertEqual(None, actual_info.credentials)
            self.assertEqual('broad-jade-dev-data-bucket', actual_info.bucket_name)
            self.assertEqual('fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
                             actual_info.key)
            self.assertEqual('HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam', actual_info.name)
            self.assertEqual(15601108255, actual_info.size)
            self.assertEqual('2020-04-27T15:56:09.696Z', actual_info.updated)

    # test for when some fields are missing in drs_resolver response
    def test_drs_resolver_response_with_missing_fields(self):
        resp_json = mock.MagicMock(return_value=self.mock_drs_resolver_response_missing_fields)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            actual_info = drs.get_drs_info(self.jade_dev_url)
            self.assertEqual({'project_id': "foo"}, actual_info.credentials)
            self.assertEqual('broad-jade-dev-data-bucket', actual_info.bucket_name)
            self.assertEqual('fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
                             actual_info.key)
            self.assertEqual(None, actual_info.name)
            self.assertEqual(None, actual_info.size)
            self.assertEqual(None, actual_info.updated)

    # test for when 'gsUrl' is missing in drs_resolver response. It should throw error
    @unittest.skip("TODO: Test no gsUri _and_ no accessUrl")
    def test_drs_resolver_response_without_gs_uri(self):
        resp_json = mock.MagicMock(return_value=self.mock_drs_resolver_response_without_gs_uri)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaisesRegex(drs.DRSResolutionError, f"No GS url found for DRS uri '{self.jade_dev_url}'"):
                drs.get_drs_blob(self.jade_dev_url)

    # test for when drs_resolver returns error. It should throw error
    def test_drs_resolver_error_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_drs_resolver_error_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=500, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaises(drs.DRSResolutionError):
                drs.get_drs_blob(self.jade_dev_url)

    # test for when drs_resolver returns error response with 'text' field. It should throw error
    def test_drs_resolver_empty_error_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_drs_resolver_empty_error_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=500, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaises(drs.DRSResolutionError):
                drs.get_drs_blob(self.jade_dev_url)

    def test_bucket_name_and_key(self):
        expected_bucket_name = f"{uuid4()}"
        random_key = f"{uuid4()}/{uuid4()}"
        for gs_url, expected_key in [(f"gs://{expected_bucket_name}/{random_key}", random_key),
                                     (f"gs://{expected_bucket_name}/", ""),
                                     (f"gs://{expected_bucket_name}", "")]:
            bucket_name, key = drs._bucket_name_and_key(gs_url)
            with self.subTest(gs_url, key=expected_key):
                self.assertEqual(expected_bucket_name, bucket_name)
                self.assertEqual(expected_key, key)

        with self.subTest("Should raise for non-valid URIs"):
            with self.assertRaises(AssertionError):
                drs._bucket_name_and_key("not a valid gs url")

    def test_get_drs_cloud_platform(self):
        cloud_platform = drs.get_drs_cloud_platform()
        self.assertEqual(cloud_platform, "gs")

    @testmode("controlled_access")
    def test_info(self):
        uri = "drs://dg.4503/3677c5b9-3c68-48a7-af1c-62056ba82d1d"
        expected_info = dict(
            name="phg001275.v1.TOPMed_WGS_MESA_v2.genotype-calls-vcf.WGS_markerset_grc38.c2.HMB-NPU.tar.gz",
            size=183312787601,
            updated="2019-12-26T20:20:39.396Z",
            checksums=dict(md5="aec4c2708e3a7ecaf6b66f12d63318ff"),
        )
        self.assertEqual(drs.info(uri), expected_info)

    @testmode("controlled_access")
    def test_access(self):
        uri = 'drs://dg.4503/3677c5b9-3c68-48a7-af1c-62056ba82d1d'
        with self.subTest(f'Testing DRS Access: {uri}'):
            signed_url = drs.access(uri)
            # Use 'Range' header to only download the first two bytes
            # https://cloud.google.com/storage/docs/json_api/v1/parameters#range
            response = requests.get(signed_url, headers={'Range': 'bytes=0-1'})
            response.raise_for_status()
        # Proteomics Data Commons (PDC) data
        pdc_uri = 'drs://dg.4DFC:dg.4DFC/00040a6f-b7e5-4e5c-ab57-ee92a0ba8201'
        with self.subTest(f'Testing DRS Access: {pdc_uri}'):
            signed_url = drs.access(pdc_uri)
            # Use 'Range' header to only download the first two bytes
            # https://cloud.google.com/storage/docs/json_api/v1/parameters#range
            response = requests.get(signed_url, headers={'Range': 'bytes=0-1'})
            response.raise_for_status()
        # Test a Jade resource
        # requires that GOOGLE_APPLICATION_CREDENTIALS be set
        # because DRSHub does not return a service account
        jade_uri = 'drs://jade-terra.datarepo-prod.broadinstitute.org/' \
                   'v1_c3c588a8-be3f-467f-a244-da614be6889a_635984f0-3267-4201-b1ee-d82f64b8e6d1'
        with self.subTest(f'Testing DRS Access: {jade_uri}'):
            signed_url = drs.access(jade_uri)
            # Use 'Range' header to only download the first two bytes
            # https://cloud.google.com/storage/docs/json_api/v1/parameters#range
            response = requests.get(signed_url, headers={'Range': 'bytes=0-1'})
            response.raise_for_status()

    @testmode("kids_first")
    def test_kids_first_access(self):
        """Kid's First can't be linked while any other projects are linked so this test must be run alone."""
        signed_url = drs.access('drs://dg.F82A1A:abe28363-7879-4c3a-95f1-e34603e8e3ee')
        # Use 'Range' header to only download the first two bytes
        # https://cloud.google.com/storage/docs/json_api/v1/parameters#range
        response = requests.get(signed_url, headers={'Range': 'bytes=0-1'})
        response.raise_for_status()

# These tests will only run on `make dev_env_access_test` command as they are testing DRS against Terra Dev env
@testmode("dev_env_access")
class TestTerraNotebookUtilsCLI_DRSInDev(CLITestMixin, unittest.TestCase):
    jade_dev_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-" \
                   "2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060"
    expected_crc32c = "/VKJIw=="

    def test_copy(self):
        with self.subTest("test copy to local path"):
            with tempfile.NamedTemporaryFile() as tf:
                self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
                               drs_url=self.jade_dev_url,
                               dst=tf.name,
                               workspace=WORKSPACE_NAME,
                               workspace_namespace=WORKSPACE_NAMESPACE)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test copy to gs bucket"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
                           drs_url=self.jade_dev_url,
                           dst=f"gs://{WORKSPACE_BUCKET}/{key}",
                           workspace=WORKSPACE_NAME,
                           workspace_namespace=WORKSPACE_NAMESPACE)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
            out = io.BytesIO()
            blob.download_to_file(out)
            blob.reload()  # download_to_file causes the crc32c to change, for some reason. Reload blob to recover.
            self.assertEqual(self.expected_crc32c, blob.crc32c)
            self.assertEqual(_crc32c(out.getvalue()), blob.crc32c)

@testmode("controlled_access")
class TestTerraNotebookUtilsCLI_DRS(CLITestMixin, unittest.TestCase):
    drs_url = DRS_URI_370_KB
    expected_crc32c = "nX4opw=="
    first_11_bytes = b"\x43\x48\x52\x4f\x4d\x09\x50\x4f\x53\x09\x49"

    def test_copy(self):
        with self.subTest("test local"):
            with tempfile.NamedTemporaryFile() as tf:
                self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
                               drs_url=self.drs_url,
                               dst=tf.name,
                               workspace=WORKSPACE_NAME,
                               workspace_namespace=WORKSPACE_NAMESPACE)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test gs"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
                           drs_url=self.drs_url,
                           dst=f"gs://{WORKSPACE_BUCKET}/{key}",
                           workspace=WORKSPACE_NAME,
                           workspace_namespace=WORKSPACE_NAMESPACE)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key)
            out = io.BytesIO()
            blob.download_to_file(out)
            blob.reload()  # download_to_file causes the crc32c to change, for some reason. Reload blob to recover.
            self.assertEqual(self.expected_crc32c, blob.crc32c)
            self.assertEqual(_crc32c(out.getvalue()), blob.crc32c)

    def test_head(self):
        def _test_head(uri: str, num_bytes: int, expected_error: Optional[Exception]=None):
            cmd = [f'{pkg_root}/dev_scripts/tnu', 'drs', 'head', uri,
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_NAMESPACE}']
            if 1 < num_bytes:
                cmd.append(f"--bytes={num_bytes}")
            with self.subTest(uri=uri, num_bytes=num_bytes, expected_error=expected_error):
                if expected_error is None:
                    stdout = self._run_cmd(cmd)
                    self.assertEqual(self.first_11_bytes[:num_bytes], stdout)
                else:
                    with self.assertRaises(expected_error):  # type: ignore
                        self._run_cmd(cmd)

        tests = [(self.drs_url, 1), (self.drs_url, 11), ("drs://nothing/asf/f", 1, subprocess.CalledProcessError)]
        with ThreadPoolExecutor() as e:
            for f in as_completed([e.submit(_test_head, *args) for args in tests]):
                f.result()

def list_bucket(prefix="", bucket=WORKSPACE_BUCKET):
    for blob in gs.get_client().bucket(bucket).list_blobs(prefix=prefix):
        yield blob.name

def _list_tree(root) -> Generator[str, None, None]:
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)

def _crc32c(data: bytes) -> str:
    # Compute Google's wonky base64 encoded crc32c checksum
    return base64.b64encode(google_crc32c.Checksum(data).digest()).decode("utf-8")

if __name__ == '__main__':
    unittest.main()
