"""
Configure the CLI
"""
import typing
import argparse

from terra_notebook_utils.cli import Config, dispatch


config_cli = dispatch.target("config", help=__doc__)


@config_cli.action("set", arguments={
    "--key": dict(choices=Config.attributes),
    "--value": dict()
})
def config_set(args: argparse.Namespace):
    """
    Set a configuration value. Use `tnu config print` to see current settings.
    """
    setattr(Config, args.key, args.value)
    Config.write()

@config_cli.action("print")
def config_print(args: argparse.Namespace):
    """
    Print the CLI configuration.
    """
    import json
    data = Config.get()
    print(json.dumps(data, indent=2))
