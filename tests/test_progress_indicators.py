#!/usr/bin/env python
import os
import sys
import unittest
import argparse
from importlib import reload
from unittest import mock
from typing import Callable

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import terra_notebook_utils.cli
from terra_notebook_utils import drs
from terra_notebook_utils.cli.commands import drs as drs_cli
from terra_notebook_utils.blobstore.progress import Indicator


class TestProgressIndicators(unittest.TestCase):
    """Test that the correct progress indicators obtain."""

    cli_args = argparse.Namespace(workspace="a", workspace_namespace="b", drs_url="c", drs_uris=["a", "b"], dst=".",
                                  manifest=None)

    def test_indicators(self):
        tests = [
            (drs_cli.drs_copy, True, "auto", Indicator.notebook_bar),
            (drs_cli.drs_copy, False, "auto", Indicator.bar),
            (drs_cli.drs_copy, True, "log", Indicator.log),
            (drs_cli.drs_copy, False, "log", Indicator.log),
            (drs_cli.drs_copy_batch, True, "auto", Indicator.notebook_bar),
            (drs_cli.drs_copy_batch, False, "auto", Indicator.log),
            (drs_cli.drs_copy_batch, True, "log", Indicator.log),
            (drs_cli.drs_copy_batch, False, "log", Indicator.log),
        ]
        for cli_method, is_notebook_env, cli_indicator_config, expected in tests:
            with self.subTest(cli_method=cli_method.__name__,
                              is_notebook_env=is_notebook_env,
                              cli_indicator_config=cli_indicator_config,
                              expected=expected.name):
                self._test_indicators(cli_method, is_notebook_env, cli_indicator_config, expected)

    def _test_indicators(self, cli_method: Callable, is_notebook_env: bool, cli_indicator_config: str,
                         expected: Indicator):
        with mock.patch("terra_notebook_utils.utils.is_notebook", return_value=is_notebook_env):
            reload(drs)
            with mock.patch("terra_notebook_utils.drs.DRSCopyClient") as mock_copy_client:
                with mock.patch("terra_notebook_utils.drs.enable_requester_pays"):
                    terra_notebook_utils.cli.CLIConfig.info['copy_progress_indicator_type'] = cli_indicator_config
                    cli_method(self.cli_args)
                    self.assertEqual(expected, mock_copy_client.call_args[1]['indicator_type'])

if __name__ == '__main__':
    unittest.main()
