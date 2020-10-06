import json
import argparse
from typing import Any, Dict

from terra_notebook_utils import workflows
from terra_notebook_utils.cli import Config, dispatch


workflow_cli = dispatch.group("workflows", help=workflows.__doc__)

workspace_args: Dict[str, Dict[str, Any]] = {
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
    "--google-billing-project": dict(
        type=str,
        required=False,
        default=Config.info['workspace_google_project'],
        help=("The billing project for GS requests. "
              "If omitted, the CLI configured `workspace_google_project` will be used. "
              "Note that DRS URLs also involve a GS request.")
    )
}

@workflow_cli.command("list-submissions", arguments={**workspace_args})
def list_submissions(args: argparse.Namespace):
    """
    List workflow submissions in chronological order
    """
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    listing = [(s['submissionId'], s['submissionDate'], s['status'])
               for s in workflows.list_submissions(args.workspace, args.google_billing_project)]
    for submission_id, date, status in sorted(listing, key=lambda item: item[1]):
        print(submission_id, date, status)

@workflow_cli.command("get-submission", arguments={
    "--submission-id": dict(type=str, required=True),
    ** workspace_args
})
def get_submission(args: argparse.Namespace):
    """
    Get information about a submission, including member worklows
    """
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    submission = workflows.get_submission(args.submission_id, args.workspace, args.google_billing_project)
    print(json.dumps(submission, indent=2))

@workflow_cli.command("get-workflow", arguments={
    "--submission-id": dict(type=str, required=True),
    "--workflow-id": dict(type=str, required=True),
    ** workspace_args
})
def get_workflow(args: argparse.Namespace):
    """
    Get information about a workflow
    """
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    wf = workflows.get_workflow(args.submission_id, args.workflow_id, args.workspace, args.google_billing_project)
    print(json.dumps(wf, indent=2))
