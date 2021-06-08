#!/usr/bin/env python
import os
import sys
import tempfile
import datetime
import unittest
from math import ceil
from uuid import uuid4
from random import randint
from typing import Dict

from getm import checksum, default_chunk_size
from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from terra_notebook_utils.blobstore import BlobStore, Part, BlobNotFoundError
from terra_notebook_utils.blobstore.gs import GSBlobStore
from terra_notebook_utils.blobstore.local import LocalBlobStore
from terra_notebook_utils.blobstore.url import URLBlobStore

from tests import infra


gs_blobstore = GSBlobStore(infra.get_env("TNU_BLOBSTORE_TEST_GS_BUCKET"))
local_test_tempdir = tempfile.TemporaryDirectory()
local_test_bucket = local_test_tempdir.name
local_blobstore = LocalBlobStore(local_test_tempdir.name)
url_blobstore = URLBlobStore()

gs_client = storage.Client.from_service_account_json(infra.get_env("TNU_GOOGLE_APPLICATION_CREDENTIALS"))

def _gen_gs_signed_url(bucket_name: str, key: str) -> str:
    blob = gs_client.bucket(bucket_name).blob(key)
    return blob.generate_signed_url(datetime.timedelta(days=1), version="v4")

def _put_blob(bs: BlobStore, data: bytes) -> str:
    key = f"{uuid4()}"
    if not isinstance(bs, URLBlobStore):
        bs.blob(key).put(data)
        return key
    else:
        gs_blobstore.blob(key).put(data)
        url = _gen_gs_signed_url(gs_blobstore.bucket_name, key)
        return url

def _fake_key(bs: BlobStore) -> str:
    if isinstance(bs, URLBlobStore):
        return f"https://{uuid4()}"
    else:
        return f"{uuid4()}"

class TestData:
    def __init__(self, oneshot_size: int=7, multipart_size: int=2 * default_chunk_size + 1):
        self.oneshot_size, self.multipart_size = oneshot_size, multipart_size
        self._oneshot: Dict[BlobStore, str] = dict()
        self._multipart: Dict[BlobStore, str] = dict()

    @property
    def oneshot_data(self):
        if not getattr(self, "_oneshot_data", None):
            self._oneshot_data = os.urandom(self.oneshot_size)
        return self._oneshot_data

    @property
    def multipart_data(self):
        if not getattr(self, "_multipart_data", None):
            self._multipart_data = os.urandom(self.multipart_size)
        return self._multipart_data

    def oneshot_blob(self, bs: BlobStore):
        if not self._oneshot.get(bs):
            if isinstance(bs, URLBlobStore):
                key = self.oneshot_blob(gs_blobstore).key
                self._oneshot[bs] = _gen_gs_signed_url(gs_blobstore.bucket_name, key)
            else:
                key = f"{uuid4()}"
                bs.blob(key).put(self.oneshot_data)
                self._oneshot[bs] = key
        return bs.blob(self._oneshot[bs])

    def multipart_blob(self, bs: BlobStore):
        if not self._multipart.get(bs):
            if isinstance(bs, URLBlobStore):
                key = self.multipart_blob(gs_blobstore).key
                self._multipart[bs] = _gen_gs_signed_url(gs_blobstore.bucket_name, key)
            else:
                key = f"{uuid4()}"
                bs.blob(key).put(self.multipart_data)
                self._multipart[bs] = key
        return bs.blob(self._multipart[bs])

test_data = TestData()

class TestBlobStore(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_schema(self):
        self.assertEqual("gs://", GSBlobStore.schema)

    def test_put_get_delete(self):
        key = f"{uuid4()}"
        expected_data = os.urandom(1021)
        for bs in (local_blobstore, gs_blobstore):
            with self.subTest(blobstore=bs, key=key):
                bs.blob(key).put(expected_data)
                self.assertEqual(expected_data, bs.blob(key).get())
                bs.blob(key).delete()
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(key).get()

        with self.subTest(blobstore=url_blobstore, key=key):
            url = _put_blob(url_blobstore, expected_data)
            self.assertEqual(expected_data, url_blobstore.blob(url).get())
            with self.assertRaises(BlobNotFoundError):
                url_blobstore.blob(_fake_key(url_blobstore)).get()

    def test_copy_from(self):
        dst_key = f"{uuid4()}"
        for bs in (local_blobstore, gs_blobstore):
            for src_blob in (test_data.oneshot_blob(bs), test_data.multipart_blob(bs)):
                with self.subTest(src_key=src_blob.key, dst_key=dst_key, blobstore=bs):
                    dst_blob = bs.blob(dst_key)
                    dst_blob.copy_from(src_blob)
                    self.assertEqual(src_blob.get(), dst_blob.get())
            with self.subTest("blob not found", blobstore=bs):
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(_fake_key(bs)).copy_from(bs.blob(_fake_key(bs)))

    def test_download(self):
        dst_path = local_blobstore.blob(f"{uuid4()}").url
        for bs in (local_blobstore, gs_blobstore, url_blobstore):
            for expected_data, src_blob in [(test_data.oneshot_data, test_data.oneshot_blob(bs)),
                                            (test_data.multipart_data, test_data.multipart_blob(bs))]:
                with self.subTest(key=src_blob.key, blobstore=bs):
                    src_blob.download(dst_path)
                    with open(dst_path, "rb") as fh:
                        data = fh.read()
                    self.assertEqual(expected_data, data)
                with self.subTest("blob not found", blobstore=bs):
                    with self.assertRaises(BlobNotFoundError):
                        bs.blob(_fake_key(bs)).download(dst_path)
                dst_subdir_path = local_blobstore.blob(os.path.join(f"{uuid4()}", "nope")).url
                with self.subTest("Subdirectories don't exist", blobstore=bs):
                    with self.assertRaises(FileNotFoundError):
                        bs.blob(src_blob.key).download(dst_subdir_path)

    def test_size(self):
        expected_size = randint(1, 509)
        data = os.urandom(expected_size)
        for bs in (local_blobstore, gs_blobstore, url_blobstore):
            key = _put_blob(bs, data)
            with self.subTest(blobstore=bs, key=key):
                self.assertEqual(expected_size, bs.blob(key).size())
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(_fake_key(bs)).size()

    def test_cloud_native_checksums(self):
        data = os.urandom(1)
        tests = [(gs_blobstore, checksum.GSCRC32C(data).gs_crc32c())]
        for bs, expected_checksum in tests:
            key = _put_blob(bs, data)
            with self.subTest(blobstore=bs, key=key):
                self.assertEqual(expected_checksum, bs.blob(key).cloud_native_checksum())
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").cloud_native_checksum()

    def test_part_iterators(self):
        tests = [("gs", gs_blobstore), ("url", url_blobstore)]
        for test_name, bs in tests:
            blob = test_data.multipart_blob(bs)
            number_of_parts = ceil(len(test_data.multipart_data) / bs.chunk_size)
            expected_parts = [test_data.multipart_data[i * bs.chunk_size:(i + 1) * bs.chunk_size]
                              for i in range(number_of_parts)]
            with self.subTest(test_name):
                count = 0
                for part_number, data in blob.iter_content():
                    self.assertEqual(expected_parts[part_number], data)
                    count += 1
                self.assertEqual(number_of_parts, count)

                zero_blob = bs.blob(_put_blob(bs, b""))
                for part_number, data in zero_blob.iter_content():
                    pass
                self.assertEqual(0, part_number)
                self.assertEqual(b"", data)

        with self.subTest("shuold raise 'BlobNotFoundError'"):
            with self.assertRaises(BlobNotFoundError):
                bs.blob(_fake_key(bs)).iter_content()

    def test_exists(self):
        for bs in (local_blobstore, gs_blobstore):
            with self.subTest(blobstore=bs):
                key = _fake_key(bs)
                self.assertFalse(bs.blob(key).exists())
                key = _put_blob(bs, b"sff")
                self.assertTrue(bs.blob(key).exists())
                if isinstance(bs, LocalBlobStore):
                    with self.assertRaises(ValueError):
                        bs.blob("/").exists()

    def test_multipart_writers(self):
        expected_data = test_data.multipart_data
        chunk_size = default_chunk_size
        number_of_chunks = ceil(len(expected_data) / chunk_size)
        for bs in (gs_blobstore,):
            with self.subTest(blobstore=bs):
                dst_blob = bs.blob(f"{uuid4()}")
                with dst_blob.multipart_writer() as writer:
                    for i in range(number_of_chunks):
                        part = Part(number=i,
                                    data=expected_data[i * chunk_size: (i + 1) * chunk_size])
                        writer.put_part(part)
                self.assertEqual(expected_data, dst_blob.get())

if __name__ == '__main__':
    unittest.main()
