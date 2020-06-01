"""
Configure the CLI
"""
import typing
import argparse

from terra_notebook_utils.cli import Config, dispatch


config_cli = dispatch.group("config", help=__doc__)


@config_cli.command("set-workspace", arguments={
    "workspace": dict()
})
def set_config_workspace(args: argparse.Namespace):
    """
    Set workspace for cli commands
    """
    Config.info["workspace"] = args.workspace
    Config.write()

@config_cli.command("set-workspace-google-project", arguments={
    "billing_project": dict()
})
def set_config_billing_project(args: argparse.Namespace):
    """
    Set billing project for cli commands
    """
    Config.info["workspace_google_project"] = args.billing_project
    Config.write()

@config_cli.command("print")
def config_print(args: argparse.Namespace):
    """
    Print the CLI configuration.
    """
    import json
    print(json.dumps(Config.info, indent=2))
