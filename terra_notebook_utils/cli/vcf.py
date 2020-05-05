"""
VCF information
"""
import typing
import argparse

from terra_notebook_utils import vcf
from terra_notebook_utils.cli import dispatch
from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT


vcf_cli = dispatch.target("vcf", help=__doc__)


@vcf_cli.action("head",
                arguments={"path": dict(),
                           "--billing-project": dict(type=str, required=False)})
def head(args: argparse.Namespace):
    """
    Print VCF header. May be a local file, a Google Storage object, or a DRS object.
    """
    if args.path.startswith("gs://"):
        from terra_notebook_utils import gs
        path = args.path.split("gs://", 1)[1] 
        bucket_name, key = path.split("/", 1)
        blob = gs.get_client().bucket(bucket_name).blob(key)
        info = vcf.VCFInfo.with_blob(blob)
    elif args.path.startswith("drs://"):
        from terra_notebook_utils import drs
        if not args.billing_project:
            print("Must pass in `billing-project` when resolving DRS url")
            return
        client, bucket_name, key = drs.resolve_drs_for_gs_storage(args.path)
        blob = client.bucket(bucket_name, user_project=args.billing_project).blob(key)
        info = vcf.VCFInfo.with_blob(blob)
    else:
        info = vcf.VCFInfo.with_file(args.path)
    info.print_header()
