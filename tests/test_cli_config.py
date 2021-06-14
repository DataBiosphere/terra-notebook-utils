#!/usr/bin/env python
import io
import os
import sys
import json
import unittest
import argparse
from uuid import uuid4
from unittest import mock
from contextlib import redirect_stdout
from tempfile import NamedTemporaryFile

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment
from tests import ConfigOverride
from tests.infra.testmode import testmode
from tests.infra import SuppressWarningsMixin
from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT
from terra_notebook_utils.cli import Config
import terra_notebook_utils.cli.commands.config


@testmode("workspace_access")
class TestTerraNotebookUtilsCLI_Config(SuppressWarningsMixin, unittest.TestCase):
    def test_config_print(self):
        workspace = f"{uuid4()}"
        workspace_namespace = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            with ConfigOverride(workspace, workspace_namespace, tf.name):
                Config.write()
                args = argparse.Namespace()
                out = io.StringIO()
                with redirect_stdout(out):
                    terra_notebook_utils.cli.commands.config.config_print(args)
                data = json.loads(out.getvalue())
                self.assertEqual(data, dict(workspace=workspace, workspace_namespace=workspace_namespace))

    def test_resolve(self):
        with self.subTest("Should fall back to env vars if arguments are None and config file missing"):
            with ConfigOverride(None, None):
                workspace, namespace = Config.resolve(None, None)
                self.assertEqual(WORKSPACE_NAME, workspace)
                self.assertEqual(WORKSPACE_GOOGLE_PROJECT, namespace)
        with self.subTest("Should fall back to config if arguments are None/False"):
            with ConfigOverride(str(uuid4()), str(uuid4())):
                workspace, namespace = Config.resolve(None, None)
                self.assertEqual(Config.info['workspace'], workspace)
                self.assertEqual(Config.info['workspace_namespace'], namespace)
        with self.subTest("Should attempt namespace resolve via fiss when workspace present, namespace empty"):
            expected_namespace = str(uuid4())
            with mock.patch("terra_notebook_utils.workspace.get_workspace_namespace", return_value=expected_namespace):
                with ConfigOverride(WORKSPACE_NAME, None):
                    terra_notebook_utils.cli.WORKSPACE_GOOGLE_PROJECT = None
                    workspace, namespace = Config.resolve(None, None)
                    self.assertEqual(Config.info['workspace'], workspace)
                    self.assertEqual(expected_namespace, namespace)
        with self.subTest("Should use overrides for workspace and workspace_namespace"):
            expected_workspace = str(uuid4())
            expected_namespace = str(uuid4())
            with mock.patch("terra_notebook_utils.workspace.get_workspace_namespace", return_value=expected_namespace):
                with ConfigOverride(str(uuid4()), str(uuid4())):
                    terra_notebook_utils.cli.WORKSPACE_GOOGLE_PROJECT = None
                    workspace, namespace = Config.resolve(expected_workspace, expected_namespace)
                    self.assertEqual(expected_workspace, workspace)
                    self.assertEqual(expected_namespace, namespace)

    def test_config_set(self):
        new_workspace = f"{uuid4()}"
        new_workspace_namespace = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            with ConfigOverride(None, None, tf.name):
                Config.write()
                args = argparse.Namespace(workspace=new_workspace)
                terra_notebook_utils.cli.commands.config.set_config_workspace(args)
                args = argparse.Namespace(workspace_namespace=new_workspace_namespace)
                terra_notebook_utils.cli.commands.config.set_config_workspace_namespace(args)
                with open(tf.name) as fh:
                    data = json.loads(fh.read())
                self.assertEqual(data, dict(workspace=new_workspace,
                                            workspace_namespace=new_workspace_namespace))

if __name__ == '__main__':
    unittest.main()
