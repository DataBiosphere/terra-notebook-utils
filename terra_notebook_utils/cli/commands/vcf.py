import json
import argparse

from terra_notebook_utils import vcf
from terra_notebook_utils.cli import dispatch, CLIConfig
from terra_notebook_utils.drs import blob_for_url


vcf_cli = dispatch.group("vcf", help=vcf.__doc__, arguments={
    "path": dict(
        help="local path, gs://, or drs://"
    ),
    "--workspace": dict(
        type=str,
        default=None,
        help="Workspace name. If not provided, the configured CLI workspace will be used."
    ),
    "--workspace-namespace": dict(
        type=str,
        required=False,
        default=CLIConfig.info['workspace_namespace'],
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
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    blob = blob_for_url(args.path, args.workspace_namespace)
    info = vcf.VCFInfo.with_blob(blob)
    info.print_header()

@vcf_cli.command("samples")
def samples(args: argparse.Namespace):
    """
    Output VCF samples.
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    blob = blob_for_url(args.path, args.workspace_namespace)
    info = vcf.VCFInfo.with_blob(blob)
    print(json.dumps(info.samples, indent=2))

@vcf_cli.command("stats")
def stats(args: argparse.Namespace):
    """
    Output VCF stats.
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    blob = blob_for_url(args.path, args.workspace_namespace)
    info = vcf.VCFInfo.with_blob(blob)
    stats = {
        'first data line chromosome': info.chrom,
        'length associated with first data line chromosome': info.length,
        'number of samples': len(info.samples),
        'size': blob.size(),
    }
    print(json.dumps(stats, indent=2))
