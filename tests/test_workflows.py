#!/usr/bin/env python
import os
import sys
import json
import unittest
from unittest import mock

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests import CLITestMixin
from tests.infra.testmode import testmode
from terra_notebook_utils import workflows
from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT
import terra_notebook_utils.cli.workflows


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

    def test_estimate_workflow_cost(self):
        workflow_id = workflows.get_submission(self.submission_id)['workflows'][0]['workflowId']
        workflows.estimate_workflow_cost(self.submission_id, workflow_id)

    def test_get_all_workflows(self):
        workflows.get_all_workflows(self.submission_id)

@testmode("workspace_access")
class TestTerraNotebookUtilsWorkflowsCLI(CLITestMixin, unittest.TestCase):
    common_kwargs = dict(workspace=WORKSPACE_NAME, workspace_namespace=WORKSPACE_GOOGLE_PROJECT)

    def test_list_submissions(self):
        with mock.patch("terra_notebook_utils.workflows.list_submissions"):
            self._test_cmd(terra_notebook_utils.cli.workflows.list_submissions)

    def test_get_submission(self):
        with mock.patch("terra_notebook_utils.workflows.get_submission", return_value=dict()):
            self._test_cmd(terra_notebook_utils.cli.workflows.get_submission,
                           submission_id="frank")

    def test_get_workflow(self):
        with mock.patch("terra_notebook_utils.workflows.get_submission", return_value=dict()):
            with mock.patch("terra_notebook_utils.workflows.get_workflow", return_value=dict()):
                self._test_cmd(terra_notebook_utils.cli.workflows.get_submission,
                               submission_id="frank",
                               workflow_id="bob")

    def test_estimate_workflow_cost(self):
        ret = dict(workflows=[dict(workflowId=1)])
        with mock.patch("terra_notebook_utils.workflows.get_all_workflows", return_value=ret):
            with mock.patch("terra_notebook_utils.workflows.estimate_workflow_cost",
                            return_value=dict()) as estimate_workflow_cost:
                self._test_cmd(terra_notebook_utils.cli.workflows.estimate_submission_cost, submission_id="frank")
                estimate_workflow_cost.assert_called()

if __name__ == '__main__':
    unittest.main()
