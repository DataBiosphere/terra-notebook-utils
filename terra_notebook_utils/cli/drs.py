import json
import argparse
from typing import Any, Dict

from terra_notebook_utils import drs
from terra_notebook_utils.cli import dispatch, Config


drs_cli = dispatch.group("drs", help=drs.__doc__)

workspace_args: Dict[str, Dict[str, Any]] = {
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
}

@drs_cli.command("copy", arguments={
    "drs_url": dict(type=str),
    "dst": dict(type=str, help="local file path, or Google Storage location if prefixed with 'gs://'"),
    ** workspace_args,
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

@drs_cli.command("copy-batch", arguments={
    "drs_urls": dict(type=str, nargs="*", help="space separated list of drs:// URIs"),
    "--dst": dict(type=str, required=True, help="local directory, or Google Storage location if prefixed with 'gs://'"),
    ** workspace_args,
})
def drs_copy_batch(args: argparse.Namespace):
    """
    Copy several drs:// objects to local directory or Google Storage bucket
    examples:
        tnu drs copy drs://my-drs-1 drs://my-drs-2 drs://my-drs-3 --dst /tmp/doom
        tnu drs copy drs://my-drs-1 drs://my-drs-2 drs://my-drs-3 --dst gs://my-cool-bucket/my-cool-folder
    """
    assert 1 <= len(args.drs_urls)
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    drs.copy_batch(args.drs_urls, args.dst, args.workspace, args.google_billing_project)

@drs_cli.command("head", arguments={
    "-c": dict(type=int, required=False, default=None,
               help="Return the first integer n bytes of a file (uncompressed)."),
    "drs_url": dict(type=str),
    ** workspace_args,
})
def drs_head(args: argparse.Namespace):
    """
    Print the first bytes of a drs:// object.

    Example:
        tnu drs head drs://crouching-drs-hidden-access
    """
    # if the user specified nothing, only fetch the first byte
    args.c = 1 if args.c is None else args.c
    assert args.c > 0

    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    drs.head(args.drs_url,
             num_bytes=args.c,
             workspace_name=args.workspace,
             google_billing_project=args.google_billing_project)

@drs_cli.command("extract-tar-gz", arguments={
    "drs_url": dict(type=str),
    "dst_gs_url": dict(type=str, help=("Root of extracted archive. This must be a Google Storage location prefixed"
                                       "prefixed with 'gs://'")),
    ** workspace_args,
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
    info = drs.resolve_drs_info_for_gs_storage(args.drs_url)
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
    info = drs.resolve_drs_info_for_gs_storage(args.drs_url)
    print(json.dumps(info.credentials, indent=2))
