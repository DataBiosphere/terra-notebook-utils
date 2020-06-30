import os
import json
import typing
import argparse

from terra_notebook_utils import table
from terra_notebook_utils.cli import dispatch, Config


table_cli = dispatch.group("table", help=table.__doc__, arguments={
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
    "--namespace": dict(
        type=str,
        default=None,
        help=("workspace namespace (google billing project). If not provided, the configured CLI google billing "
              "project will be used.")
    ),
})


@table_cli.command("list")
def list_tables(args: argparse.Namespace):
    """
    List all tables, with column headers, in the workspace
    """
    args.workspace, args.namespace = Config.resolve(args.workspace, args.namespace)
    out = dict()
    for t, attributes in table.list_tables(args.workspace, args.namespace):
        out[t] = attributes
    print(json.dumps(out, indent=2))

@table_cli.command("get", arguments={
    "--table": dict(type=str, required=True, help="table name")
})
def get_table(args: argparse.Namespace):
    """
    Get all rows
    """
    args.workspace, args.namespace = Config.resolve(args.workspace, args.namespace)
    for e in table.list_entities(args.table, args.workspace, args.namespace):
        data = e['attributes']
        data[f'{args.table}_id'] = e['name']
        print(json.dumps(data, indent=2))

@table_cli.command("get-row", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--id": dict(type=str, required=True, help="table name"),
})
def get_row(args: argparse.Namespace):
    """
    Get one row
    """
    args.workspace, args.namespace = Config.resolve(args.workspace, args.namespace)
    e = table.get_row(args.table, args.id, args.workspace, args.namespace)
    data = e['attributes']
    data[f'{args.table}_id'] = e['name']
    print(json.dumps(data, indent=2))

@table_cli.command("get-cell", arguments={
    "--table": dict(type=str, required=True, help="table name"),
    "--id": dict(type=str, required=True, help="id of entity"),
    "--column": dict(type=str, required=True, help="column name"),
})
def get_cell(args: argparse.Namespace):
    """
    Get cell value
    """
    args.workspace, args.namespace = Config.resolve(args.workspace, args.namespace)
    for e in table.list_entities(args.table, args.workspace, args.namespace):
        if args.id == e['name']:
            print(e['attributes'][args.column])

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
    args.workspace, args.namespace = Config.resolve(args.workspace, args.namespace)
    headers = [f"{args.table}_id"]
    values = [args.id]
    for pair in args.data:
        key, val = pair.split("=")
        headers.append(key)
        values.append(val)
    tsv = "\t".join(headers) + os.linesep + "\t".join(values)
    table.upload_entities(tsv, args.workspace, args.namespace)
