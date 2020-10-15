import json
import typing
import argparse
from datetime import datetime
from typing import Any, Tuple, List, Dict

from firecloud import fiss

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

@workflow_cli.command("estimate-submission-cost", arguments={
    "--submission-id": dict(type=str, required=True),
    ** workspace_args
})
def estimate_submission_cost(args: argparse.Namespace):
    """
    Estimate costs for all workflows in a submission
    """
    args.workspace, args.google_billing_project = Config.resolve(args.workspace, args.google_billing_project)
    submission = workflows.get_submission(args.submission_id, args.workspace, args.google_billing_project)
    reporter = TXTReport([("workflow_id", 37),
                          ("shard", 6),
                          ("cpus", 5),
                          ("memory (GB)", 12),
                          ("duration (h)", 13),
                          ("cost", 5)])
    reporter.print_headers()
    total = 0
    for wf in submission['workflows']:
        workflow_id = wf['workflowId']
        shard = 1
        for item in workflows.estimate_workflow_cost(args.submission_id,
                                                     workflow_id,
                                                     args.workspace,
                                                     args.google_billing_project):
            cost, cpus, mem, duration = (item[k] for k in ('cost', 'number_of_cpus', 'memory', 'duration'))
            reporter.print_line(workflow_id, shard, cpus, mem, duration / 3600, cost)
            total += cost
            shard += 1
    reporter.print_divider()
    reporter.print_line("", "", "", "", "", total)

class TXTReport:
    def __init__(self, fields: List[Tuple[str, int]]):
        self.column_headers = [f[0] for f in fields]
        self.widths = [f[1] for f in fields]

    @property
    def width(self) -> int:
        return len(self.line(*[str() for _ in self.widths]))

    def line(self, *vals) -> str:
        assert len(vals) == len(self.widths), f"Expected {len(self.widths)} values"
        parts = list()
        for val, width in zip(vals, self.widths):
            parts.append(self.ff(val, width))
        return " ".join(parts)

    def print_headers(self):
        print(self.line(*self.column_headers))

    def print_line(self, *vals):
        print(self.line(*vals))

    def print_divider(self):
        print("-" * self.width)

    def ff(self, val: Any, width: int, decimals: int=2) -> str:
        if isinstance(val, str):
            return f"%{width}s" % val
        elif isinstance(val, int):
            return f"%{width}i" % val
        elif isinstance(val, float):
            return f"%{width}.2f" % round(val, decimals)
        else:
            raise TypeError(f"Unsupported type '{type(val)}'")
