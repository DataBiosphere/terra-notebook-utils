#!/usr/bin/env python
import io
import os
import sys
import unittest
import tempfile
import contextlib
from uuid import uuid4
from unittest import mock
from typing import Generator

import gs_chunked_io as gscio

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET, WORKSPACE_NAME
from terra_notebook_utils import drs, gs, tar_gz, vcf
from terra_notebook_utils.drs import DRSResolutionError
from contextlib import ExitStack
from tests.infra import SuppressWarningsMixin


# These tests will only run on `make dev_env_access_test` command as they are testing DRS against Terra Dev env
@testmode("dev_env_access")
class TestTerraNotebookUtilsDRSInDev(SuppressWarningsMixin, unittest.TestCase):
    jade_dev_url = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-" \
                   "2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060"

    def test_resolve_drs_for_google_storage(self):
        _, info = drs.resolve_drs_for_gs_storage(self.jade_dev_url)
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

    def test_download(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy_to_local(self.jade_dev_url, tf.name)

    def test_copy_to_local(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy(self.jade_dev_url, tf.name)

    def test_multipart_copy(self):
        with mock.patch("terra_notebook_utils.MULTIPART_THRESHOLD", 1024 * 1024):
            drs.copy_to_bucket(self.jade_dev_url, "test_oneshot_object")

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

    @testmode("controlled_access")
    def test_resolve_drs_for_google_storage(self):
        _, info = drs.resolve_drs_for_gs_storage(self.drs_url)
        self.assertEqual(info.bucket_name, "topmed-irc-share")
        self.assertEqual(info.key, "genomes/NWD522743.b38.irc.v1.cram.crai")

    @testmode("controlled_access")
    def test_download(self):
        with tempfile.NamedTemporaryFile() as tf:
            drs.copy_to_local(self.drs_url, tf.name)

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
    def test_oneshot_copy(self):
        drs.copy_to_bucket(self.drs_url, "test_oneshot_object")

    @testmode("controlled_access")
    def test_multipart_copy(self):
        with mock.patch("terra_notebook_utils.MULTIPART_THRESHOLD", 1024 * 1024):
            drs.copy_to_bucket(self.drs_url, "test_oneshot_object")

    @testmode("controlled_access")
    def test_copy(self):
        with self.subTest("Test copy to local location"):
            with tempfile.NamedTemporaryFile() as tf:
                drs.copy(self.drs_url, tf.name)
                self.assertTrue(os.path.isfile(tf.name))
        with self.subTest("Test copy to bucket location"):
            key = f"test_oneshot_object_{uuid4()}"
            drs.copy(self.drs_url, f"gs://{WORKSPACE_BUCKET}/{key}")
            self.assertTrue(self._gs_obj_exists(key))
        with self.subTest("Test copy to bare bucket"):
            name = drs.info(self.drs_url)['name']
            drs.copy(self.drs_url, f"gs://{WORKSPACE_BUCKET}")
            self.assertTrue(self._gs_obj_exists(name))

    def _gs_obj_exists(self, key: str) -> bool:
        return gs.get_client().bucket(WORKSPACE_BUCKET).blob(key).exists()

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

    @testmode("controlled_access")
    def test_extract_tar_gz(self):
        expected_data = (b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00\x90 \xed]kO\\I\x92\xfd\xcc\xfc"
                         b"\x8a\xd2\xb4V\xfb2\xd7\xf9~\xac\x97\x910\xb6i$\x1b\xbb\r\xdb=\xd3_\x10\x862\xae\x1d\x0c"
                         b"\x0cU\xb8\xa7G\xfe\xf1{\xe2\xc6\xc9\xa2\xc0\xb8\xdb\xdd\xf2,_\xd2R\xc8\x87")
        # This test uses a hack property, `_extract_single_chunk`, to extract a small amount
        # of data from the cohort vcf pointed to by `drs://dg.4503/da8cb525-4532-4d0f-90a3-4d327817ec73`.
        with mock.patch("terra_notebook_utils.tar_gz._extract_single_chunk", True):
            drs_url = "drs://dg.4503/da8cb525-4532-4d0f-90a3-4d327817ec73"  # cohort VCF tarball
            pfx = f"test_cohort_extract_{uuid4()}"
            drs.extract_tar_gz(drs_url, pfx)
            for key in gs.list_bucket(pfx):
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
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.copy"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.gscio"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.tar_gz"))
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
                    drs.extract_tar_gz(self.drs_url, "some_pfx", "some_bucket")
                    enable_requester_pays.assert_called_with(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT)

    # test for when we get everything what we wanted in martha_v3 response
    def test_martha_v3_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_jdr_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            _, actual_info = drs.resolve_drs_for_gs_storage(self.jade_dev_url)
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
            _, actual_info = drs.resolve_drs_for_gs_storage(self.drs_url)
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
            _, actual_info = drs.resolve_drs_for_gs_storage(self.jade_dev_url)
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
            _, actual_info = drs.resolve_drs_for_gs_storage(self.drs_url)
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
            with self.assertRaisesRegex(DRSResolutionError, f"No GS url found for DRS uri '{self.jade_dev_url}'"):
                drs.resolve_drs_for_gs_storage(self.jade_dev_url)

    # test for when no GS url is found in martha_v2 response. It should throw error
    def test_martha_v2_response_without_gs_uri(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v2_response_without_gs_uri)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=200, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaisesRegex(Exception, f"No GS url found for DRS uri '{self.drs_url}'"):
                drs.resolve_drs_for_gs_storage(self.drs_url)

    # test for when martha_v3 returns error. It should throw error
    def test_martha_v3_error_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v3_error_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=500, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaisesRegex(
                    DRSResolutionError,
                    "Unexpected response while resolving DRS path. Expected status 200, got 500. "
                    "Error: {\"msg\":\"User 'null' does not have required action: read_data\",\"status_code\":500}"
            ):
                drs.resolve_drs_for_gs_storage(self.jade_dev_url)

    # test for when martha_v3 returns error response with 'text' field. It should throw error
    def test_martha_v3_empty_error_response(self):
        resp_json = mock.MagicMock(return_value=self.mock_martha_v3_empty_error_response)
        requests_post = mock.MagicMock(return_value=mock.MagicMock(status_code=500, json=resp_json))
        with ExitStack() as es:
            es.enter_context(mock.patch("terra_notebook_utils.drs.gs.get_client"))
            es.enter_context(mock.patch("terra_notebook_utils.drs.http", post=requests_post))
            with self.assertRaisesRegex(
                    DRSResolutionError,
                    "Unexpected response while resolving DRS path. Expected status 200, got 500. "
            ):
                drs.resolve_drs_for_gs_storage(self.jade_dev_url)

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

def _list_tree(root) -> Generator[str, None, None]:
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)

if __name__ == '__main__':
    unittest.main()
