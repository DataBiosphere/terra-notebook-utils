#!/usr/bin/env python
import os
import sys
import logging
import tempfile
import unittest
from unittest import mock
from uuid import uuid4
from typing import Dict, Iterable, Optional

from getm import default_chunk_size

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from terra_notebook_utils import WORKSPACE_BUCKET
from terra_notebook_utils.blobstore import BlobNotFoundError
from terra_notebook_utils.blobstore.gs import GSBlobStore, GSBlob
from terra_notebook_utils.blobstore.local import LocalBlobStore, LocalBlob
from terra_notebook_utils.blobstore.url import URLBlob
from terra_notebook_utils.blobstore import BlobStore, copy_client

from tests import infra


gs_blobstore = GSBlobStore(infra.get_env("TNU_BLOBSTORE_TEST_GS_BUCKET"))
local_test_tempdir = tempfile.TemporaryDirectory()
local_test_bucket = local_test_tempdir.name
local_blobstore = LocalBlobStore(local_test_tempdir.name)

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
copy_client.logger.setLevel(logging.DEBUG)

class TestData:
    def __init__(self, oneshot_size: int=7, multipart_size: int=2 * default_chunk_size + 1):
        self.oneshot_size = oneshot_size
        self.multipart_size = multipart_size

        self._oneshot: Optional[bytes] = None
        self._multipart: Optional[bytes] = None
        self._oneshot_key = f"{uuid4()}"
        self._multipart_key = f"{uuid4()}"
        self._uploaded: Dict[BlobStore, bool] = dict()

    @property
    def oneshot(self):
        self._oneshot = self._oneshot or os.urandom(self.oneshot_size)
        return self._oneshot

    @property
    def multipart(self):
        self._multipart = self._multipart or os.urandom(self.multipart_size)
        return self._multipart

    def uploaded(self, dests: Iterable[BlobStore]):
        oneshot = dict(key=self._oneshot_key, data=self.oneshot)
        multipart = dict(key=self._multipart_key, data=self.multipart)
        for bs in dests:
            if not self._uploaded.get(bs):
                for data_config in [oneshot, multipart]:
                    bs.blob(data_config['key']).put(data_config['data'])
                self._uploaded[bs] = True
        return oneshot, multipart

test_data = TestData()

class TestExceptionOfDoom(Exception):
    pass

def _mock_do_copy(*args, **kwargs):
    raise TestExceptionOfDoom()

class TestCopyClient(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_copy(self):
        src_blob = mock.MagicMock()
        dst_blob = mock.MagicMock()
        with mock.patch("terra_notebook_utils.blobstore.copy_client.copy") as copy_method:
            copy_client.copy(src_blob, dst_blob)
            copy_method.assert_called_once()

    def test_copy_client(self):
        with self.subTest("should work"):
            # The copy client is general enough to upload local files to cloud locations. However, TNU is not expected
            # to handle this use case, and the copy client contains assertions to prevent this. For now.
            expected_data_map, completed_keys = self._do_blobstore_copies(src_blobstores=[gs_blobstore])
            self.assertEqual(len(expected_data_map), len(completed_keys))
            for blob, expected_data in expected_data_map.items():
                with self.subTest(blob.url):
                    self.assertEqual(blob.get(), expected_data)

    def test_copy_client_error_handling(self):
        src_blob = GSBlob("doom", "gloom")
        src_blob.size = lambda: 10
        dst_blob = GSBlob(f"no-such-bucket-{uuid4()}", "no-such-key")

        with mock.patch("terra_notebook_utils.blobstore.copy_client._do_copy", _mock_do_copy):
            raise_on_error = False
            with self.subTest("Copy operation should fail. Client should not raise"):
                with copy_client.CopyClient(raise_on_error=raise_on_error) as client:
                    client.copy(src_blob, dst_blob)
            raise_on_error = True
            with self.subTest("Copy operation should fail. Client should raise"):
                with self.assertRaises(TestExceptionOfDoom):
                    with copy_client.CopyClient(raise_on_error=raise_on_error) as client:
                        client.copy(src_blob, dst_blob)

    def _do_blobstore_copies(self,
                             src_blobstores=(local_blobstore, gs_blobstore),
                             dst_blobstores=(local_blobstore, gs_blobstore),
                             ignore_missing_checksums=True,
                             compute_checksums=False):
        oneshot, multipart = test_data.uploaded([local_blobstore, gs_blobstore])
        expected_data_map = dict()
        completed_keys = list()
        with copy_client.CopyClient() as client:
            for src_bs in src_blobstores:
                for dst_bs in dst_blobstores:
                    for data_bundle in (oneshot, multipart):
                        src_blob = src_bs.blob(data_bundle['key'])
                        dst_blob = dst_bs.blob(os.path.join(f"{uuid4()}", f"{uuid4()}"))
                        completed_keys.append(dst_blob.key)
                        if compute_checksums:
                            client.copy_compute_checksums(src_blob, dst_blob)
                        else:
                            client.copy(src_blob, dst_blob)
                        expected_data_map[dst_blob] = data_bundle['data']
        return expected_data_map, completed_keys

    def test_blob_for_url(self):
        bucket_name = f"{uuid4()}"
        key = "ba/ba/little/black/sheep"
        tests = [
            (f"gs://{bucket_name}/{key}", GSBlob),
            ("http://oompa/loompa", URLBlob),
            ("https://oompa/loompa", URLBlob),
            ("foo/bar", LocalBlob),
            (os.path.sep.join(["", "argle", "bargle"]), LocalBlob),
        ]
        for url, expected_blob_class in tests:
            with self.subTest(url=url, cls=expected_blob_class):
                blob = copy_client.blob_for_url(url)
                self.assertTrue(isinstance(blob, expected_blob_class))
                if isinstance(blob, GSBlob):
                    self.assertEqual(bucket_name, blob.bucket_name)
                    self.assertEqual(key, blob.key)
                if isinstance(blob, LocalBlob):
                    self.assertEqual(os.path.abspath(url), blob.key)

        with self.subTest("should error"):
            tests = ["gs://bucket-name"]
            for url in tests:
                with self.assertRaises(ValueError):
                    copy_client.blob_for_url(url)

if __name__ == '__main__':
    unittest.main()
