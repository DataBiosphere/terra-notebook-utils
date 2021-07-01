"""
Configure the CLI
"""
import typing
import argparse

from terra_notebook_utils.cli import CLIConfig, dispatch


config_cli = dispatch.group("config", help=__doc__)


@config_cli.command("set-workspace", arguments={
    "workspace": dict()
})
def set_config_workspace(args: argparse.Namespace):
    """
    Set workspace for cli commands
    """
    CLIConfig.info["workspace"] = args.workspace
    CLIConfig.write()

@config_cli.command("set-workspace-namespace", arguments={
    "workspace_namespace": dict(type=str)
})
def set_config_workspace_namespace(args: argparse.Namespace):
    """
    Set workspace namespace for cli commands
    """
    CLIConfig.info["workspace_namespace"] = args.workspace_namespace
    CLIConfig.write()

@config_cli.command("set-copy-progress-indicator-type", arguments={
    "copy_progress_indicator_type": dict(type=str, choices=["auto", "log"])
})
def set_indicator_type(args: argparse.Namespace):
    """
    Set the indicator type for DRS copy operations.

    When 'copy-progress-indicator-type' is set to 'auto', terra-notebook-utils chooses the most appropriate
    indicator type for copy operations.
    """
    CLIConfig.info["copy_progress_indicator_type"] = args.copy_progress_indicator_type
    CLIConfig.write()

@config_cli.command("print")
def config_print(args: argparse.Namespace):
    """
    Print the CLI configuration.
    """
    import json
    print(json.dumps(CLIConfig.info, indent=2))
