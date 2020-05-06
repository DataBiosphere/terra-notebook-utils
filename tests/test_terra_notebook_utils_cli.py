#!/usr/bin/env python
import io
import os
import sys
import json
import unittest
import argparse
from uuid import uuid4
from contextlib import redirect_stdout
from tempfile import NamedTemporaryFile

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import terra_notebook_utils.cli.config
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


if __name__ == '__main__':
    unittest.main()
