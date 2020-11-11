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

@config_cli.command("set-workspace-namespace", arguments={
    "workspace_namespace": dict(type=str)
})
def set_config_workspace_namespace(args: argparse.Namespace):
    """
    Set workspace namespace for cli commands
    """
    Config.info["workspace_namespace"] = args.workspace_namespace
    Config.write()

@config_cli.command("print")
def config_print(args: argparse.Namespace):
    """
    Print the CLI configuration.
    """
    import json
    print(json.dumps(Config.info, indent=2))
