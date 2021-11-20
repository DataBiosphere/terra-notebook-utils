import sys
import json
import argparse
from typing import Any, Dict

from terra_notebook_utils import drs
from terra_notebook_utils.cli import dispatch, CLIConfig
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
        default=CLIConfig.info['workspace_namespace'],
        help=("The workspace namespace represents the parent containing the workspace "
              "(the Terra billing project) "
              "If omitted, the CLI configured `workspace_namespace` will be used. ")
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

    If 'dst' is suffixed with "/", the destination is assumed to be a folder and the file name is
    derived from the drs response and appended to 'dst'. Otherwise the destination is assumed
    to be absolute.

    examples:
        tnu drs copy drs://my-drs-id /tmp/doom  # copy to /tmp/doom
        tnu drs copy drs://my-drs-id /tmp/doom/  # copy to /tmp/doom/{file-name-from-drs-resolution}
        tnu drs copy drs://my-drs-id gs://my-cool-bucket/my-cool-bucket-key
        tnu drs copy drs://my-drs-id gs://my-cool-bucket/my-cool-bucket-key/
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    kwargs: Dict[str, Any] = dict(workspace_name=args.workspace, workspace_namespace=args.workspace_namespace)
    if CLIConfig.progress_indicator_type() is not None:
        kwargs['indicator_type'] = CLIConfig.progress_indicator_type()
    drs.copy(args.drs_url, args.dst, **kwargs)

@drs_cli.command("copy-batch", arguments={
    "drs_uris": dict(type=str, nargs="*", help="space separated list of drs:// URIs"),
    "--dst": dict(type=str, default=None, help="local directory, or Google Storage location if prefixed with 'gs://'"),
    "--manifest": dict(type=str, default=None, help="filepath to JSON manifest."),
    ** workspace_args,
})
def drs_copy_batch(args: argparse.Namespace):
    """
    Copy several drs:// objects to local directory or Google Storage bucket
    examples:
        tnu drs copy-batch drs://my-drs-1 drs://my-drs-2 drs://my-drs-3 --dst /tmp/doom/
        tnu drs copy-batch drs://my-drs-1 drs://my-drs-2 drs://my-drs-3 --dst gs://my-cool-bucket/my-cool-folder
        tnu drs copy-batch --manifest manifest.json

    When not using a manifest, 'dst' is treated as a folder, and file names are derived from the drs response.
    Otherwise, in a manifest, 'dst' can either be a folder (if suffixed with "/"), or an absolute path, e.g.
    '/home/me/my-file-name.vcf.gz' or 'gs://bucket-name/pfx/my-file.vcf.gz'.

    example manifest.json:
    [
      {
        "drs_uri": "drs://my/cool/drs/uri",
        "dst": "/path/to/local/dir/"
      },
      {
        "drs_uri": "drs://my/cool/drs/uri",
        "dst": "gs://my-cook-bucket/my-cool-prefix"
      }
    ]
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    kwargs: Dict[str, Any] = dict(workspace_name=args.workspace, workspace_namespace=args.workspace_namespace)
    if CLIConfig.progress_indicator_type() is not None:
        kwargs['indicator_type'] = CLIConfig.progress_indicator_type()
    if args.drs_uris:
        assert args.manifest is None, "Cannot use 'drs_uris' with '--manifest'"
        assert args.dst is not None, "Must specify a destination with '--dst'"
        drs.copy_batch_urls(args.drs_uris, args.dst, **kwargs)
    elif args.manifest:
        with open(args.manifest) as fh:
            manifest = json.loads(fh.read())
        drs.copy_batch_manifest(manifest, **kwargs)
    else:
        raise RuntimeError("Must supply either 'drs_uris' or '--manifest'")

@drs_cli.command("head", arguments={
    "drs_url": dict(type=str),
    "--bytes": dict(type=int, required=False, default=1, help="Number of bytes to fetch."),
    ** workspace_args,
})
def drs_head(args: argparse.Namespace):
    """
    Print the first bytes of a drs:// object.

    Example:
        tnu drs head drs://crouching-drs-hidden-access
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    the_bytes = drs.head(args.drs_url,
                         num_bytes=args.bytes,
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
    "dst": dict(type=str,
                nargs='?',
                default=None,
                help=("Root of extracted archive. This may be a local filepath or a 'gs://' url."
                      "If not provided, default extraction is to the bucket for 'workspace'")),
    ** workspace_args,
})
def drs_extract_tar_gz(args: argparse.Namespace):
    """
    Extract a `tar.gz` archive resolved by DRS into a Google Storage bucket.
    example:
        tnu drs extract-tar-gz drs://my-tar-gz gs://my-dst-bucket/root
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    drs.extract_tar_gz(args.drs_url, args.dst, args.workspace, args.workspace_namespace)

@drs_cli.command("info", arguments={
    "drs_url": dict(type=str),
})
def drs_info(args: argparse.Namespace):
    """
    Get information about a drs:// URI
    """
    info = drs.info(args.drs_url)
    print(json.dumps(info, indent=2))

@drs_cli.command("access", arguments={
    "drs_url": dict(type=str),
    ** workspace_args,
})
def drs_access(args: argparse.Namespace):
    """
    Get a signed url for a drs:// URI
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    signed_url = drs.access(args.drs_url, args.workspace, args.workspace_namespace)
    print(signed_url)

@drs_cli.command("credentials", arguments={
    "drs_url": dict(type=str),
})
def drs_credentials(args: argparse.Namespace):
    """
    Return the credentials needed to access a DRS url.
    """
    try:
        info = drs.get_drs_info(args.drs_url)
    except DRSResolutionError:
        raise
    except Exception:
        raise

    print(json.dumps(info.credentials, indent=2))
