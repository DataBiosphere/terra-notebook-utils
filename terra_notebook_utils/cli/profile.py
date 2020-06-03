import json
import argparse

from terra_notebook_utils import profile
from terra_notebook_utils.cli import dispatch


profile_cli = dispatch.group("profile", help=profile.__doc__)


@profile_cli.command("list-billing-projects")
def list_billing_projects(args: argparse.Namespace):
    """
    Billing projects available to the current usuer
    """
    print(json.dumps(profile.list_billing_projects(), indent=2))
