#!/usr/bin/env python
import io
import os
import sys
import json
import typing
import unittest
import argparse
from uuid import uuid4
from contextlib import redirect_stdout, redirect_stderr
from tempfile import NamedTemporaryFile

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
from tests.infra import testmode
from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, WORKSPACE_BUCKET
import terra_notebook_utils.cli.config
import terra_notebook_utils.cli.vcf
import terra_notebook_utils.cli.workspace
import terra_notebook_utils.cli.profile
from terra_notebook_utils.cli import Config
from terra_notebook_utils.cli import TNUCommandDispatch


class TestTerraNotebookUtilsCLI(unittest.TestCase):
    def test_dispatch(self):
        with self.subTest("dispatch without mutually exclusive arguments"):
            self._test_dispatch()

        with self.subTest("dispatch with mutually exclusive arguments"):
            self._test_dispatch(mutually_exclusive=True)

        with self.subTest("dispatch with action overrides"):
            self._test_dispatch(action_overrides=True)

    def _test_dispatch(self, mutually_exclusive=None, action_overrides=False):
        dispatch = TNUCommandDispatch()
        target = dispatch.target(
            "my_target",
            arguments={
                "foo": dict(default="george", type=int),
                "--argument-a": None,
                "--argument-b": dict(default="bar"),
            },
            mutually_exclusive=(["--argument-a", "--argument-b"] if mutually_exclusive else None)
        )

        if action_overrides:
            @target.action("my_action", arguments={"foo": None, "--bar": dict(default="bars")})
            def my_action(args):
                self.assertEqual(args.argument_b, "LSDKFJ")
                self.assertEqual(args.foo, "24")
                self.assertEqual(args.bar, "bars")
        else:
            @target.action("my_action")
            def my_action(args):
                self.assertEqual(args.argument_b, "LSDKFJ")
                self.assertEqual(args.foo, 24)

        dispatch(["my_target", "my_action", "24", "--argument-b", "LSDKFJ"])

    def test_config_print(self):
        workspace = f"{uuid4()}"
        workspace_google_project = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            Config._path = tf.name
            Config.workspace = workspace
            Config.workspace_google_project = workspace_google_project
            Config.write()
            args = argparse.Namespace()
            out = io.StringIO()
            with redirect_stdout(out):
                terra_notebook_utils.cli.config.config_print(args)
            data = json.loads(out.getvalue())
            self.assertEqual(data, dict(workspace=workspace, workspace_google_project=workspace_google_project))

    def test_config_set(self):
        new_workspace = f"{uuid4()}"
        new_workspace_google_project = f"{uuid4()}"
        with NamedTemporaryFile() as tf:
            Config._path = tf.name
            Config.write()
            args = argparse.Namespace(key="workspace", value=new_workspace)
            terra_notebook_utils.cli.config.config_set(args)
            args = argparse.Namespace(key="workspace_google_project", value=new_workspace_google_project)
            terra_notebook_utils.cli.config.config_set(args)
            with open(tf.name) as fh:
                data = json.loads(fh.read())
            self.assertEqual(data, dict(workspace=new_workspace, workspace_google_project=new_workspace_google_project))


class _CLITestCase(unittest.TestCase):
    common_kwargs: dict = dict()

    def _test_cmd(self, cmd: typing.Callable, **kwargs):
        with NamedTemporaryFile() as tf:
            Config._path = tf.name
            Config.workspace = WORKSPACE_NAME  # type: ignore
            Config.workspace_google_project = WORKSPACE_GOOGLE_PROJECT  # type: ignore
            Config.write()
            args = argparse.Namespace(**dict(**self.common_kwargs, **kwargs))
            out = io.StringIO()
            with redirect_stderr(out):
                with redirect_stdout(out):
                    cmd(args)


class TestTerraNotebookUtilsCLI_VCF(_CLITestCase):
    common_kwargs = dict(google_billing_project=WORKSPACE_GOOGLE_PROJECT)

    def test_head(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head,
                           path="gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
                                "/consent1"
                                "/HVH_phs000993_TOPMed_WGS_freeze.8.chr10.hg38.vcf.gz")

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode.controlled_access
    def test_head_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.head, path="drs://dg.4503/32c380e0-c196-47c7-8a69-6e4370ac9fc7")

    def test_samples(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples,
                           path="gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
                                "/consent1"
                                "/HVH_phs000993_TOPMed_WGS_freeze.8.chr10.hg38.vcf.gz")

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode.controlled_access
    def test_samples_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.samples,
                           path="drs://dg.4503/32c380e0-c196-47c7-8a69-6e4370ac9fc7")

    def test_stats(self):
        with self.subTest("Test gs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats,
                           path="gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
                                "/consent1"
                                "/HVH_phs000993_TOPMed_WGS_freeze.8.chr10.hg38.vcf.gz")

        with self.subTest("Test local object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats, path="tests/fixtures/non_block_gzipped.vcf.gz")

    @testmode.controlled_access
    def test_stats_drs(self):
        with self.subTest("Test drs:// object"):
            self._test_cmd(terra_notebook_utils.cli.vcf.stats,
                           path="drs://dg.4503/32c380e0-c196-47c7-8a69-6e4370ac9fc7")


class TestTerraNotebookUtilsCLI_Workspace(_CLITestCase):
    def test_list(self):
        self._test_cmd(terra_notebook_utils.cli.workspace.list_workspaces)

    def test_get(self):
        self._test_cmd(terra_notebook_utils.cli.workspace.get_workspace,
                       workspace=WORKSPACE_NAME,
                       namespace="firecloud-cgl")


class TestTerraNotebookUtilsCLI_Profile(_CLITestCase):
    def test_list_billing_projects(self):
        self._test_cmd(terra_notebook_utils.cli.profile.list_billing_projects)


if __name__ == '__main__':
    unittest.main()
