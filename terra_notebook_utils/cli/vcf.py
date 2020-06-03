import json
import typing
import argparse

from terra_notebook_utils import vcf
from terra_notebook_utils.cli import dispatch, Config
import google.cloud.storage.blob


vcf_cli = dispatch.group("vcf", help=vcf.__doc__, arguments={
    "path": dict(
        help="local path, gs://, or drs://"
    ),
    "--google-billing-project": dict(
        type=str,
        required=False,
        default=Config.info['workspace_google_project'],
        help=("The billing project for GS requests. "
              "If omitted, the CLI configured `workspace_google_project` will be used. "
              "Note that DRS URLs also involve a GS request.")
    ),
})


@vcf_cli.command("head")
def head(args: argparse.Namespace):
    """
    Output VCF header.
    """
    blob = _get_blob(args.path, args.google_billing_project)
    if blob:
        info = vcf.VCFInfo.with_blob(blob)
    else:
        info = vcf.VCFInfo.with_file(args.path)
    info.print_header()

@vcf_cli.command("samples")
def samples(args: argparse.Namespace):
    """
    Output VCF samples.
    """
    blob = _get_blob(args.path, args.google_billing_project)
    if blob:
        info = vcf.VCFInfo.with_blob(blob)
    else:
        info = vcf.VCFInfo.with_file(args.path)
    print(json.dumps(info.samples, indent=2))

@vcf_cli.command("stats")
def stats(args: argparse.Namespace):
    """
    Output VCF stats.
    """
    blob = _get_blob(args.path, args.google_billing_project)
    if blob:
        info = vcf.VCFInfo.with_blob(blob)
        size = blob.size
    else:
        import os
        info = vcf.VCFInfo.with_file(args.path)
        size = os.path.getsize(os.path.abspath(args.path))
    stats = {
        'first data line chromosome': info.chrom,
        'length associated with first data line chromosome': info.length,
        'number of samples': len(info.samples),
        'size': size
    }
    print(json.dumps(stats, indent=2))

def _get_blob(path: str, google_project: str) -> google.cloud.storage.blob:
    if path.startswith("gs://"):
        from terra_notebook_utils import gs
        path = path.split("gs://", 1)[1]
        bucket_name, key = path.split("/", 1)
        blob = gs.get_client(project=google_project).bucket(bucket_name).get_blob(key)
    elif path.startswith("drs://"):
        from terra_notebook_utils import drs
        client, bucket_name, key = drs.resolve_drs_for_gs_storage(path)
        blob = client.bucket(bucket_name, user_project=google_project).get_blob(key)
    else:
        blob = None
    return blob
