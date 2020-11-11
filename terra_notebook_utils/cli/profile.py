import json
import argparse

from terra_notebook_utils import profile
from terra_notebook_utils.cli import dispatch


profile_cli = dispatch.group("profile", help=profile.__doc__)


@profile_cli.command("list-workspace-namespaces")
def list_workspace_namespaces(args: argparse.Namespace):
    """
    Workspace namespaces available to the current usuer
    """
    print(json.dumps(profile.list_workspace_namespaces(), indent=2))
