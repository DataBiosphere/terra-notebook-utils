import json
import argparse

from terra_notebook_utils import drs
from terra_notebook_utils.cli import dispatch, Config


drs_cli = dispatch.group("drs", help=drs.__doc__, arguments={
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
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
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    drs.copy(args.drs_url, args.dst, args.workspace, args.google_billing_project)

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
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    drs.extract_tar_gz(args.drs_url, pfx, bucket, args.workspace, args.google_billing_project)

@drs_cli.command("info", arguments={
    "drs_url": dict(type=str),
})
def drs_info(args: argparse.Namespace):
    """
    Get information about drs:// objects
    """
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    info = drs.resolve_drs_info_for_gs_storage(args.drs_url, args.workspace, args.google_billing_project)
    data = info._asdict()
    data['url'] = f"gs://{info.bucket_name}/{info.key}"
    del data['credentials']
    del data['bucket_name']
    del data['key']
    print(json.dumps(data, indent=2))

@drs_cli.command("credentials", arguments={
    "drs_url": dict(type=str),
})
def drs_credentials(args: argparse.Namespace):
    """
    Return the credentials needed to access a DRS url.
    """
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    info = drs.resolve_drs_info_for_gs_storage(args.drs_url, args.workspace, args.google_billing_project)
    print(json.dumps(info.credentials, indent=2))
