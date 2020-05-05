"""
Configure the CLI
"""
import typing
import argparse

from terra_notebook_utils.cli import Config, dispatch
from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT


config_cli = dispatch.target("config", help=__doc__)


@config_cli.action("set", arguments={"key": dict(choices=Config.attributes), "val": dict()})
def config_set(args: argparse.Namespace):
    setattr(Config, args.key, args.val)
    Config.write()


@config_cli.action("print")
def config_print(args: argparse.Namespace):
    for key in Config.attributes:
        val = getattr(Config, key)
        print(key, val)
