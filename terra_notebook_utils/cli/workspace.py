"""
Workspace information and operations
"""
import json
import typing
import argparse

from firecloud import fiss

from terra_notebook_utils import workspace
from terra_notebook_utils.cli import Config, dispatch


workspace_cli = dispatch.target("workspace", help=__doc__)


@workspace_cli.action("list")
def list_workspaces(args: argparse.Namespace):
    """
    List workspaces available to the current usuer
    """
    data = workspace.list_workspaces()
    info_keys = ["name", "createdBy", "bucketName", "namespace"]
    workspaces = [{key: ws['workspace'][key] for key in info_keys}
                  for ws in data]
    print(json.dumps(workspaces, indent=2))

@workspace_cli.action("get", arguments={
    "--workspace": dict(type=str, required=True, help="workspace name"),
    "--namespace": dict(type=str, required=False, default=None, help="workspace namespace"),
})
def get_workspace(args: argparse.Namespace):
    """
    Get information about a workspace
    """
    data = workspace.get_workspace(args.workspace, args.namespace)
    print(json.dumps(data, indent=2))

@workspace_cli.action("delete-workflow-logs", arguments={
    "--workspace": dict(type=str,
                        default=Config.workspace,
                        help="If ommitted, the CLI configured workspace will be used.")
})
def delete_workflow_logs(args: argparse.Namespace):
    """
    THIS CANNOT BE UNDONE
    Delete _ALL_ workflow exeuction logs from the workspace bucket.
    """
    bucket = workspace.get_workspace_bucket(args.workspace)
    workspace.remove_workflow_logs(bucket)
