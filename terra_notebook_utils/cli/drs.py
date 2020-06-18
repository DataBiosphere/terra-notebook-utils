import argparse

from terra_notebook_utils import drs
from terra_notebook_utils.cli import dispatch, Config


drs_cli = dispatch.group("drs", help=drs.__doc__, arguments={
    "--google-billing-project": dict(
        type=str,
        required=False,
        default=Config.info['workspace_google_project'],
        help=("The billing project for GS requests. "
              "If omitted, the CLI configured `workspace_google_project` will be used. "
              "Note that DRS URLs also involve a GS request.")
    )
})


@drs_cli.command("copy", arguments={
    "drs_url": dict(type=str),
    "dst": dict(type=str, help="local file path, or Google Storage location if prefixed with 'gs://'"),
})
def drs_copy(args: argparse.Namespace):
    """
    Copy drs:// object to local file or Google Storage bucket
    examples:
        tnu drs copy drs://my-drs-id /tmp/doom
        tnu drs copy drs://my-drs-id gs://my-cool-bucket/my-cool-bucket-key
    """
    _, args.google_billing_project = Config.resolve(None, args.google_billing_project)
    drs.copy(args.drs_url, args.dst, args.google_billing_project)

@drs_cli.command("extract-tar-gz", arguments={
    "drs_url": dict(type=str),
    "dst_gs_url": dict(type=str, help=("Root of extracted archive. This must be a Google Storage location prefixed"
                                       "prefixed with 'gs://'")),
})
def drs_extract_tar_gz(args: argparse.Namespace):
    """
    Extract a `tar.gz` archive resolved by DRS into a Google Storage bucket.
    example:
        tnu drs extract-tar-gz drs://my-tar-gz gs://my-dst-bucket/root
    """
    assert args.dst_gs_url.startswith("gs://")
    bucket, pfx = args.dst_gs_url[5:].split("/", 1)
    pfx = pfx or None
    _, args.google_billing_project = Config.resolve(None, args.google_billing_project)
    drs.extract_tar_gz(args.drs_url, pfx, bucket, args.google_billing_project)
