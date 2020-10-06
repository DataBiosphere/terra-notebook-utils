#!/usr/bin/env python
import os
import sys
import json
import unittest

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
from tests.infra.testmode import testmode
from terra_notebook_utils import workflows


@testmode("workspace_access")
class TestTerraNotebookUtilsWorkflows(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.submissions = [s for s in workflows.list_submissions()]
        assert cls.submissions, "No submissions available in test workspace. Maybe run a workflow?"
        cls.submission_id = cls.submissions[0]['submissionId']

    def test_get_submission(self):
        workflows.get_submission(self.submission_id)

    def test_get_workflow(self):
        workflow_id = workflows.get_submission(self.submission_id)['workflows'][0]['workflowId']
        workflows.get_workflow(self.submission_id, workflow_id)

if __name__ == '__main__':
    unittest.main()
