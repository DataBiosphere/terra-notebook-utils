#!/usr/bin/env python
import io
import os
import sys
import base64
import unittest
import tempfile
import subprocess
from uuid import uuid4
from unittest import mock
from contextlib import ExitStack
from typing import Generator

import jsonschema
import google_crc32c

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests import CLITestMixin, ConfigOverride
from tests.infra import SuppressWarningsMixin, get_env
from tests.infra.testmode import testmode
from terra_notebook_utils import drs, gs, WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME
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
    drs_url = "drs://dg.4503/95cc4ae1-dee7-4266-8b97-77cf46d83d35"
    jade_dev_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-2a2bfaa24c7a_" \
                   "c0e40912-8b14-43f6-9a2f-b278144d0060"

    # martha_v3 responses
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
    mock_martha_v3_response_missing_fields = {
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
    mock_martha_v3_response_without_gs_uri = {
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
    mock_martha_v3_error_response = {
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
    mock_martha_v3_empty_error_response = {
        "status": 500,
        "response": {
            "status": 500,
        }
    }

    # martha_v2 responses
    mock_martha_v2_response = {
        'dos': {
            'data_object': {
                'aliases': [],
                'checksums': [
                    {
                        'checksum': "8a366443",
                        'type': "crc32c"
                    }, {
                        'checksum': "336ea55913bc261b72875bd259753046",
                        'type': "md5"
                    }
                ],
                'created': "2020-04-27T15:56:09.696Z",
                'description': "",
                'id': "dg.4503/00e6cfa9-a183-42f6-bb44-b70347106bbe",
                'name': "my_data",
                'mime_type': "",
                'size': 15601108255,
                'updated': "2020-04-27T15:56:09.696Z",
                'urls': [
                    {
                        'url': 's3://my_bucket/my_data'
                    },
                    {
                        'url': 'gs://bogus/my_data'
                    }
                ],
                'version': "6d60cacf"
            }
        },
        'googleServiceAccount': {
            'data': {
                'project_id': "foo"
            }
        }
    }
    mock_martha_v2_response_missing_fields = {
        'dos': {
            'data_object': {
                'checksums': [
                    {
                        'checksum': "8a366443",
                        'type': "crc32c"
                    }, {
                        'checksum': "336ea55913bc261b72875bd259753046",
                        'type': "md5"
                    }
                ],
                'created': "2020-04-27T15:56:09.696Z",
                'description': "",
                'id': "dg.4503/00e6cfa9-a183-42f6-bb44-b70347106bbe",
                'mime_type': "",
                'urls': [
                    {
                        'url': 'gs://bogus/my_data'
                    }
                ],
                'version': "6d60cacf"
            }
        }
    }
    mock_martha_v2_response_without_gs_uri = {
        'dos': {
            'data_object': {
                'checksums': [
                    {
                        'checksum': "8a366443",
                        'type': "crc32c"
                    }, {
                        'checksum': "336ea55913bc261b72875bd259753046",
                        'type': "md5"
                    }
                ],
                'created': "2020-04-27T15:56:09.696Z",
                'description': "",
                'id': "dg.4503/00e6cfa9-a183-42f6-bb44-b70347106bbe",
                'mime_type': "",
                'version': "6d60cacf"
            }
        }
    }

    def test_resolve_targets(self):
        expected_name = f"{uuid4()}"
        for name in (expected_name, None):
            info = drs.DRSInfo(credentials=None,
                               access_url=None,
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
        drs_url = 'drs://dg.4503/828d82a1-e6cd-4a24-a593-f7e8025c7d71'
        the_bytes = drs.head(drs_url)
        self.assertEqual(1, len(the_bytes))

        the_bytes = drs.head(drs_url, num_bytes=10)
        self.assertEqual(10, len(the_bytes))

        with self.assertRaises(drs.GSBlobInaccessible):
            fake_drs_url = 'drs://nothing'
            drs.head(fake_drs_url)

    @testmode("controlled_access")
    def test_download(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy(self.drs_url, tf.name)

    @testmode("controlled_access")
    def test_copy(self):
        with self.subTest("Test copy to local location"):
            with tempfile.NamedTemporaryFile() as tf:
                drs.copy(self.drs_url, tf.name)
                self.assertTrue(os.path.isfile(tf.name))
        with self.subTest("Test copy to bucket location"):
            key = f"test_oneshot_object_{uuid4()}"
            drs.copy(self.drs_url, f"gs://{TNU_TEST_GS_BUCKET}/{key}")
            self.assertTrue(self._gs_obj_exists(key))
        with self.subTest("Test copy to bare bucket"):
            name = drs.info(self.drs_url)['name']
            drs.copy(self.drs_url, f"gs://{TNU_TEST_GS_BUCKET}")
            self.assertTrue(self._gs_obj_exists(name))

    def _gs_obj_exists(self, key: str) -> bool:
        return gs.get_client().bucket(TNU_TEST_GS_BUCKET).blob(key).exists()

    @testmode("controlled_access")
    def test_copy_batch(self):
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
            drs.copy_batch(manifest)
            for name in dict(**drs_uris, **named_drs_uris):
                blob = bucket.get_blob(f"{pfx}/{name}")
                self.assertGreater(blob.size, 0)
            names = [os.path.basename(path) for path in _list_tree(dirname)]
            self.assertEqual(sorted(names), sorted(list(dict(**drs_uris, **named_drs_uris).keys())))

        with self.subTest("malformed manifest"):
            manifest = [dict(a="b"), dict(drs_uri="drs://foo", dst=".")]
            with self.assertRaises(jsonschema.exceptions.ValidationError):
                drs.copy_batch(manifest)

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
    def test_arg_propagation(self):
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
                with self.subTest("Copy to local"):
                    with tempfile.NamedTemporaryFile() as tf:
                        drs.copy(self.drs_url, tf.name)
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)
                with self.subTest("Copy to bucket"):
                    enable_requester_pays.reset_mock()
                    drs.copy(self.drs_url, "gs://some_bucket/some_key")
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)
                with self.subTest("Extract tarball"):
                    enable_requester_pays.reset_mock()
                    drs.extract_tar_gz(self.drs_url)
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)

    # test for when we get everything what we wanted in martha_v3 response
    def test_martha_v3_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_jdr_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            actual_info = drs.get_drs_info(self.jade_dev_url)
            self.assertEqual(None, actual_info.credentials)
            self.assertEqual('broad-jade-dev-data-bucket', actual_info.bucket_name)
            self.assertEqual('fd8d8492-ad02-447d-b54e-35a7ffd0e7a5/8b07563a-542f-4b5c-9e00-e8fe6b1861de',
                             actual_info.key)
            self.assertEqual('HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam', actual_info.name)
            self.assertEqual(15601108255, actual_info.size)
            self.assertEqual('2020-04-27T15:56:09.696Z', actual_info.updated)

    # test for when we get everything what we wanted in martha_v2 response
    def test_martha_v2_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v2_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            actual_info = drs.get_drs_info(self.drs_url)
            self.assertEqual({'project_id': "foo"}, actual_info.credentials)
            self.assertEqual('bogus', actual_info.bucket_name)
            self.assertEqual('my_data', actual_info.key)
            self.assertEqual('my_data', actual_info.name)
            self.assertEqual(15601108255, actual_info.size)
            self.assertEqual('2020-04-27T15:56:09.696Z', actual_info.updated)

    # test for when some fields are missing in martha_v3 response
    def test_martha_v3_response_with_missing_fields(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v3_response_missing_fields)
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

    # test for when some fields are missing in martha_v2 response
    def test_martha_v2_response_with_missing_fields(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v2_response_missing_fields)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            actual_info = drs.get_drs_info(self.drs_url)
            self.assertEqual(None, actual_info.credentials)
            self.assertEqual('bogus', actual_info.bucket_name)
            self.assertEqual('my_data', actual_info.key)
            self.assertEqual(None, actual_info.name)
            self.assertEqual(None, actual_info.size)
            self.assertEqual(None, actual_info.updated)

    # test for when 'gsUrl' is missing in martha_v3 response. It should throw error
    def test_martha_v3_response_without_gs_uri(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v3_response_without_gs_uri)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaisesRegex(drs.DRSResolutionError, f"No GS url found for DRS uri '{self.jade_dev_url}'"):
                drs.get_drs_blob(self.jade_dev_url)

    # test for when no GS url is found in martha_v2 response. It should throw error
    def test_martha_v2_response_without_gs_uri(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v2_response_without_gs_uri)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaisesRegex(Exception, f"No GS url found for DRS uri '{self.drs_url}'"):
                drs.get_drs_blob(self.drs_url)

    # test for when martha_v3 returns error. It should throw error
    def test_martha_v3_error_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v3_error_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=500, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaises(drs.DRSResolutionError):
                drs.get_drs_blob(self.jade_dev_url)

    # test for when martha_v3 returns error response with 'text' field. It should throw error
    def test_martha_v3_empty_error_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v3_empty_error_response)
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

    @testmode("controlled_access")
    def test_info(self):
        uri = "drs://dg.4503/3677c5b9-3c68-48a7-af1c-62056ba82d1d"
        expected_info = dict(
            name="phg001275.v1.TOPMed_WGS_MESA_v2.genotype-calls-vcf.WGS_markerset_grc38.c2.HMB-NPU.tar.gz",
            size=183312787601,
            updated="2019-12-26T20:20:39.396Z",
            url=("gs://nih-nhlbi-topmed-released-phs001416-c2/"
                 "phg001275.v1.TOPMed_WGS_MESA_v2.genotype-calls-vcf.WGS_markerset_grc38.c2.HMB-NPU.tar.gz")
        )
        self.assertEqual(drs.info(uri), expected_info)

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
                               workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test copy to gs bucket"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
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
            with tempfile.NamedTemporaryFile() as tf:
                self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
                               drs_url=self.drs_url,
                               dst=tf.name,
                               workspace=WORKSPACE_NAME,
                               workspace_namespace=WORKSPACE_GOOGLE_PROJECT)
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                self.assertEqual(_crc32c(data), self.expected_crc32c)

        with self.subTest("test gs"):
            key = "test-drs-cli-object"
            self._test_cmd(terra_notebook_utils.cli.commands.drs.drs_copy,
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
            cmd = [f'{pkg_root}/dev_scripts/tnu', 'drs', 'head', self.drs_url,
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
            stdout = self._run_cmd(cmd)
            self.assertEqual(stdout, b'\x1f', stdout)
            self.assertEqual(len(stdout), 1, stdout)

            cmd = [f'{pkg_root}/dev_scripts/tnu', 'drs', 'head', self.drs_url,
                   '--bytes=3',
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
            stdout = self._run_cmd(cmd)
            self.assertEqual(stdout, b'\x1f\x8b\x08')
            self.assertEqual(len(stdout), 3)

            for buffer in [1, 2, 10, 11]:
                cmd = [f'{pkg_root}/dev_scripts/tnu', 'drs', 'head', self.drs_url,
                       '--bytes=10',
                       f'--buffer={buffer}',
                       f'--workspace={WORKSPACE_NAME} ',
                       f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
                stdout = self._run_cmd(cmd)
                self.assertEqual(stdout, b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03')
                self.assertEqual(len(stdout), 10)

        with self.subTest("Test heading a non-existent drs url."):
            fake_drs_url = 'drs://nothing/asf/f'
            cmd = [f'{pkg_root}/dev_scripts/tnu', 'drs', 'head', fake_drs_url,
                   f'--workspace={WORKSPACE_NAME}',
                   f'--workspace-namespace={WORKSPACE_GOOGLE_PROJECT}']
            with self.assertRaises(subprocess.CalledProcessError):
                self._run_cmd(cmd)

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
