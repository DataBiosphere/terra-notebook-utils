import io
import os
import sys
import argparse
import subprocess
from contextlib import redirect_stdout
from tempfile import NamedTemporaryFile
from typing import List, Callable

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
from terra_notebook_utils.cli import CLIConfig
from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT

class CLIConfigOverride:
    def __init__(self, workspace, namespace, path=None):
        self.old_workspace = CLIConfig.info['workspace']
        self.old_namespace = CLIConfig.info['workspace_namespace']
        self.old_path = CLIConfig.path
        self.workspace = workspace
        self.namespace = namespace
        self.path = path

    def __enter__(self):
        CLIConfig.info['workspace'] = self.workspace
        CLIConfig.info['workspace_namespace'] = self.namespace
        CLIConfig.path = self.path

    def __exit__(self, *args, **kwargs):
        CLIConfig.info['workspace'] = self.old_workspace
        CLIConfig.info['workspace_namespace'] = self.old_namespace
        CLIConfig.path = self.old_path

class CLITestMixin:
    common_kwargs: dict = dict()

    def _test_cmd(self, cmd: Callable, **kwargs):
        with NamedTemporaryFile() as tf:
            with CLIConfigOverride(WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, tf.name):
                CLIConfig.write()
                args = argparse.Namespace(**dict(**self.common_kwargs, **kwargs))
                out = io.StringIO()
                with redirect_stdout(out):
                    cmd(args)
                return out.getvalue().strip()

    @staticmethod
    def _run_cmd(cmd: List[str]) -> bytes:
        p = subprocess.run(cmd, capture_output=True, check=True)
        return p.stdout
