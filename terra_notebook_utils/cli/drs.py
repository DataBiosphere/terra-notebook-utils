"""
DRS utilities
"""
import argparse

from terra_notebook_utils import drs
from terra_notebook_utils.cli import dispatch, Config


drs_cli = dispatch.group("drs", help=__doc__)


@drs_cli.command("cp", arguments={
    "drs_url": dict(type=str),
    "dst": dict(type=str),
    "--google-billing-project": dict(
        type=str,
        required=False,
        default=Config.info['workspace_google_project'],
        help=("The billing project for GS requests. "
              "If omitted, the CLI configured `workspace_google_project` will be used. "
              "Note that DRS URLs also involve a GS request.")
    ),
})
def drs_cp(args: argparse.Namespace):
    """
    Copy drs:// object to local file or Google Storage bucket
    examples:
        tnu drs cp drs://my-drs-id /tmp/doom
        tnu drs cp drs://my-drs-id gs://my-cool-bucket/my-cool-bucket-key
    """
    drs.copy(args.drs_url, args.dst, args.google_billing_project)
