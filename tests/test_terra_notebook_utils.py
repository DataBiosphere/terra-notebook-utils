#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests.infra.testmode import testmode
from terra_notebook_utils import WORKSPACE_BUCKET, WORKSPACE_NAME
from terra_notebook_utils import gs, progress, workspace
from tests.infra import SuppressWarningsMixin


@testmode("workspace_access")
class TestTerraNotebookUtilsProgress(SuppressWarningsMixin, unittest.TestCase):
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
class TestTerraNotebookUtilsGS(SuppressWarningsMixin, unittest.TestCase):
    def test_list_bucket(self):
        for key in gs.list_bucket("consent1"):
            print(key)


@testmode("workspace_access")
class TestTerraNotebookUtilsWorkspace(SuppressWarningsMixin, unittest.TestCase):
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

if __name__ == '__main__':
    unittest.main()
