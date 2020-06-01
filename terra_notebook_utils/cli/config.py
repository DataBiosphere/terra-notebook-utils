"""
Configure the CLI
"""
import typing
import argparse

from terra_notebook_utils.cli import Config, dispatch


config_cli = dispatch.group("config", help=__doc__)


@config_cli.command("set-workspace", arguments={
    "value": dict()
})
def set_config_workspace(args: argparse.Namespace):
    """
    Set workspace for cli commands
    """
    Config.info["workspace"] = args.value
    Config.write()

@config_cli.command("set-workspace-google-project", arguments={
    "value": dict()
})
def set_config_billing_project(args: argparse.Namespace):
    """
    Set billing project for cli commands
    """
    Config.info["workspace_google_project"] = args.value
    Config.write()

@config_cli.command("print")
def config_print(args: argparse.Namespace):
    """
    Print the CLI configuration.
    """
    import json
    print(json.dumps(Config.info, indent=2))
