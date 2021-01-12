import os
import json
import typing
import argparse

from terra_notebook_utils import table as tnu_table
from terra_notebook_utils.cli import dispatch, Config


table_cli = dispatch.group("table", help=tnu_table.__doc__, arguments={
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
    "--workspace-namespace": dict(
        type=str,
        default=None,
        help=("workspace namespace. If not provided, the configured CLI google billing "
              "project will be used.")
    ),
})

@table_cli.command("list")
def list_tables(args: argparse.Namespace):
    """
    List all tables in the workspace
    """
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    kwargs = dict(workspace_name=args.workspace, workspace_google_project=args.workspace_namespace)
    for table in tnu_table.list_tables(**kwargs):
        print(table)

@table_cli.command("list-rows", arguments={
    "--table": dict(type=str, required=True, help="table name")
})
def list_rows(args: argparse.Namespace):
    """
    Get all rows
    """
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    kwargs = dict(workspace_name=args.workspace, workspace_google_project=args.workspace_namespace)
    for row in tnu_table.list_rows(args.table, **kwargs):
        print(row.name, row.attributes)

@table_cli.command("get-row", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--row": dict(type=str, required=True, help="row name"),
})
def get_row(args: argparse.Namespace):
    """
    Get one row
    """
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    kwargs = dict(workspace_name=args.workspace, workspace_google_project=args.workspace_namespace)
    row = tnu_table.get_row(args.table, args.row, **kwargs)
    if row is not None:
        print(row.name, json.dumps(row.attributes))

@table_cli.command("fetch-drs-url", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--file-name": dict(type=str, required=True, help="file name"),
})
def fetch_drs_url(args: argparse.Namespace):
    """
    Fetch the DRS URL associated with `--file-name` in `--table`.
    """
    kwargs = dict(workspace_name=args.workspace, workspace_google_project=args.workspace_namespace)
    print(tnu_table.fetch_drs_url(args.table, args.file_name, **kwargs))

@table_cli.command("put-row", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--id": dict(type=str, required=True, help="Row id. This should be unique for the table"),
    "data": dict(type=str, nargs="+", help="list of key-value pairs")
})
def put_row(args: argparse.Namespace):
    """
    Put a row.
    Example:
    tnu table put-row \\
    --table abbrv_merge_input \\
    --id 1 \\
    bucket=fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10 \\
    input_keys=test_vcfs/a.vcf.gz,test_vcfs/b.vcf.gz \\
    output_key=foo.vcf.gz
    """
    args.workspace, args.workspace_namespace = Config.resolve(args.workspace, args.workspace_namespace)
    headers = [f"{args.table}_id"]
    values = [args.id]
    for pair in args.data:
        key, val = pair.split("=")
        headers.append(key)
        values.append(val)
    tsv = "\t".join(headers) + os.linesep + "\t".join(values)
    tnu_table.upload_entities(tsv, args.workspace, args.workspace_namespace)
