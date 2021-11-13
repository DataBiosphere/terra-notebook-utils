import json
import argparse

from terra_notebook_utils import vcf, WORKSPACE_GOOGLE_PROJECT
from terra_notebook_utils.cli import dispatch
from terra_notebook_utils.drs import blob_for_url


vcf_cli = dispatch.group("vcf", help=vcf.__doc__, arguments={
    "path": dict(
        help="local path, gs://, or drs://"
    ),
    "--billing-project": dict(
        type=str,
        required=False,
        default=WORKSPACE_GOOGLE_PROJECT,
        help=("The billing project for GS requests. "
              "If omitted, the environment variables GOOGLE_PROJECT, "
              "GCP_PROJECT, AND GCLOUD_PROJECT will be in that order.")
    ),
})

@vcf_cli.command("head")
def head(args: argparse.Namespace):
    """
    Output VCF header.
    """
    blob = blob_for_url(args.path, args.billing_project)
    info = vcf.VCFInfo.with_blob(blob)
    info.print_header()

@vcf_cli.command("samples")
def samples(args: argparse.Namespace):
    """
    Output VCF samples.
    """
    blob = blob_for_url(args.path, args.billing_project)
    info = vcf.VCFInfo.with_blob(blob)
    print(json.dumps(info.samples, indent=2))

@vcf_cli.command("stats")
def stats(args: argparse.Namespace):
    """
    Output VCF stats.
    """
    blob = blob_for_url(args.path, args.billing_project)
    info = vcf.VCFInfo.with_blob(blob)
    stats = {
        'first data line chromosome': info.chrom,
        'length associated with first data line chromosome': info.length,
        'number of samples': len(info.samples),
        'size': blob.size(),
    }
    print(json.dumps(stats, indent=2))
