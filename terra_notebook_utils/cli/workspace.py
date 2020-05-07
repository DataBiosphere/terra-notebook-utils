"""
Information about Terra workspaces
"""
import json
import typing
import argparse

from firecloud import fiss

from terra_notebook_utils.cli import Config, dispatch


workspace = dispatch.target("workspace", help=__doc__)


@workspace.action("list")
def list_workspaces(args: argparse.Namespace):
    """
    List workspaces available to the current usuer
    """
    resp = fiss.fapi.list_workspaces()
    resp.raise_for_status()
    info_keys = ["name", "createdBy", "bucketName", "namespace"]
    workspaces = [{key: ws['workspace'][key] for key in info_keys}
                  for ws in resp.json()]
    print(json.dumps(workspaces, indent=2))

@workspace.action("get", arguments={
    "--workspace": dict(type=str, required=True, help="workspace name"),
    "--namespace": dict(type=str, required=True, help="workspace namespace"),
})
def get_workspace(args: argparse.Namespace):
    """
    Get information about a workspace
    """
    resp = fiss.fapi.get_workspace(args.namespace, args.workspace)
    resp.raise_for_status()
    print(json.dumps(resp.json(), indent=2))
