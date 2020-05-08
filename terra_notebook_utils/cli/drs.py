"""
DRS utilities
"""
import json
import typing
import argparse

from terra_notebook_utils import drs
from terra_notebook_utils.cli import dispatch, Config
import google.cloud.storage.blob


drs_cli = dispatch.target("drs", help=__doc__)


@drs_cli.action("cp", arguments={
    "drs_url": dict(type=str),
    "dst": dict(type=str),
    "--google-billing-project": dict(
        type=str,
        required=False,
        default=Config.workspace_google_project,
        help=("The billing project for GS requests. "
              "If omitted, the CLI configured `workspace_google_project` will be used. "
              "Note that DRS URLs also involve a GS request.")
    ),
})
def drs_cp(args: argparse.Namespace):
    """
    Copy drs:// object to gs bucket or local filesystem
    examples:
        tnu drs cp drs://my-drs-id /tmp/doom
        tnu drs cp drs://my-drs-id gs://my-cool-bucket/my-cool-bucket-key
    """
    assert args.drs_url.startswith("drs://")
    if args.dst.startswith("gs://"):
        parts = args.dst[5:].split("/", 1)
        if 1 >= len(parts):
            raise Exception("gs:// url should contain bucket name and key, with '/' delimeter.")
        bucket_name, key = parts
        drs.copy(args.drs_url, key, bucket_name, google_billing_project=args.google_billing_project)
    else:
        drs.download(args.drs_url, args.dst, google_billing_project=args.google_billing_project)
