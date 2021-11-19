import json
import argparse
from uuid import uuid4

from terra_notebook_utils import table as tnu_table
from terra_notebook_utils.cli import dispatch, CLIConfig


table_cli = dispatch.group("table", help=tnu_table.__doc__, arguments={
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
    "--workspace-namespace": dict(
        type=str,
        default=None,
        help=("The workspace namespace represents the parent containing the workspace "
              "(the Terra billing project) "
              "If omitted, the CLI configured `workspace_namespace` will be used. ")
    ),
})

@table_cli.command("list")
def list_tables(args: argparse.Namespace):
    """
    List all tables in the workspace
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    for table in tnu_table.list_tables(args.workspace, args.workspace_namespace):
        print(table)

@table_cli.command("list-rows", arguments={
    "--table": dict(type=str, required=True, help="table name")
})
def list_rows(args: argparse.Namespace):
    """
    Get all rows
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    for row in tnu_table.list_rows(args.table, args.workspace, args.workspace_namespace):
        print(json.dumps({f"{args.table}_id": row.name, **row.attributes}))

@table_cli.command("get-row", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--row": dict(type=str, required=True, help="row name"),
})
def get_row(args: argparse.Namespace):
    """
    Get one row
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    row = tnu_table.get_row(args.table, args.row, args.workspace, args.workspace_namespace)
    if row is not None:
        print(json.dumps({f"{args.table}_id": row.name, **row.attributes}))

@table_cli.command("delete-table", arguments={
    "--table": dict(type=str, required=True, help="table name"),
})
def delete_table(args: argparse.Namespace):
    """
    Get one row
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    tnu_table.delete(args.table, args.workspace, args.workspace_namespace)

@table_cli.command("delete-row", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--row": dict(type=str, required=True, help="row name"),
})
def delete_row(args: argparse.Namespace):
    """
    Delete a row
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    tnu_table.del_row(args.table, args.row, args.workspace, args.workspace_namespace)

@table_cli.command("fetch-drs-url", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--file-name": dict(type=str, required=True, help="file name"),
})
def fetch_drs_url(args: argparse.Namespace):
    """
    Fetch the DRS URL associated with `--file-name` in `--table`.
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    print(tnu_table.fetch_drs_url(args.table, args.file_name, args.workspace, args.workspace_namespace))

@table_cli.command("put-row", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--row": dict(type=str, required=False, default=None, help="row name. This should be unique for the table"),
    "data": dict(type=str, nargs="+", help="list of key-value pairs")
})
def put_row(args: argparse.Namespace):
    """
    Put a row.
    Example:
    tnu table put-row \\
    --table abbrv_merge_input \\
    bucket=fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10 \\
    input_keys=test_vcfs/a.vcf.gz,test_vcfs/b.vcf.gz \\
    output_key=foo.vcf.gz
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    attributes = dict()
    for pair in args.data:
        key, val = pair.split("=")
        attributes[key] = val
    row = tnu_table.Row(name=args.row or f"{uuid4()}", attributes=attributes)
    tnu_table.put_row(args.table, row, args.workspace, args.workspace_namespace)
    print(json.dumps({f"{args.table}_id": row.name, **row.attributes}))
