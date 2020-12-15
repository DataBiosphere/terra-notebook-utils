import sys
import json
import argparse
from typing import Any, Dict

from terra_notebook_utils import drs, MULTIPART_THRESHOLD
from terra_notebook_utils.cli import dispatch, Config
from terra_notebook_utils.drs import DRSResolutionError


drs_cli = dispatch.group("drs", help=drs.__doc__)

workspace_args: Dict[str, Dict[str, Any]] = {
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
    "--workspace-namespace": dict(
        type=str,
        required=False,
        default=Config.info['workspace_namespace'],
        help=("The billing project for GS requests. "
              "If omitted, the CLI configured `workspace_namespace` will be used. "
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
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    drs.copy(args.drs_url, args.dst, args.workspace, args.workspace_namespace)

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
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    drs.copy_batch(args.drs_urls, args.dst, args.workspace, args.workspace_namespace)

@drs_cli.command("head", arguments={
    "drs_url": dict(type=str),
    "--bytes": dict(type=int, required=False, default=1, help="Number of bytes to fetch."),
    "--buffer": dict(type=int, required=False, default=MULTIPART_THRESHOLD,
                     help="Control the buffer size when fetching data."),
    ** workspace_args,
})
def drs_head(args: argparse.Namespace):
    """
    Print the first bytes of a drs:// object.

    Example:
        tnu drs head drs://crouching-drs-hidden-access
    """
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    the_bytes = drs.head(args.drs_url,
                         num_bytes=args.bytes,
                         buffer=args.buffer,
                         workspace_name=args.workspace,
                         workspace_namespace=args.workspace_namespace)
    # sys.stdout.buffer is used outside of a python notebook since that's the standard non-lossy way
    # to write/display bytes; sys.stdout.buffer is not available inside of a python notebook
    # though, as sys.stdout is a ipykernel.iostream.OutStream object:
    # https://github.com/ipython/ipykernel/blob/master/ipykernel/iostream.py#L265
    # so we use bare sys.stdout and rely on the ipykernel method's lossy unicode stream
    stdout_buffer = getattr(sys.stdout, 'buffer', sys.stdout)
    stdout_buffer.write(the_bytes)

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
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    drs.extract_tar_gz(args.drs_url, pfx, bucket, args.workspace, args.workspace_namespace)

@drs_cli.command("info", arguments={
    "drs_url": dict(type=str),
})
def drs_info(args: argparse.Namespace):
    """
    Get information about drs:// objects
    """
    info = drs.info(args.drs_url)
    print(json.dumps(info, indent=2))

@drs_cli.command("credentials", arguments={
    "drs_url": dict(type=str),
})
def drs_credentials(args: argparse.Namespace):
    """
    Return the credentials needed to access a DRS url.
    """
    try:
        info = drs.resolve_drs_info_for_gs_storage(args.drs_url)
    except DRSResolutionError:
        raise
    except Exception:
        raise

    print(json.dumps(info.credentials, indent=2))
