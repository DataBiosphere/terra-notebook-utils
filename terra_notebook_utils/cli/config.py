"""
Configure the CLI
"""
import typing
import argparse

from terra_notebook_utils.cli import Config, dispatch


config_cli = dispatch.group("config", help=__doc__)


@config_cli.command("set", arguments={
    "--key": dict(choices=Config.info.keys()),
    "--value": dict()
})
def config_set(args: argparse.Namespace):
    """
    Set a configuration value. Use `tnu config print` to see current settings.
    """
    Config.info[args.key] = args.value
    Config.write()

@config_cli.command("print")
def config_print(args: argparse.Namespace):
    """
    Print the CLI configuration.
    """
    import json
    print(json.dumps(Config.info, indent=2))
