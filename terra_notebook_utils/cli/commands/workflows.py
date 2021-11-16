import json
import typing
import argparse
from datetime import datetime
from typing import Any, Tuple, List, Dict

from firecloud import fiss

from terra_notebook_utils import workflows
from terra_notebook_utils.cli import CLIConfig, dispatch


workflow_cli = dispatch.group("workflows", help=workflows.__doc__)

workspace_args: Dict[str, Dict[str, Any]] = {
    "--workspace": dict(
        type=str,
        default=None,
        help="workspace name. If not provided, the configured CLI workspace will be used"
    ),
    "--workspace-namespace": dict(
        type=str,
        required=False,
        default=CLIConfig.info['workspace_namespace'],
        help=("The workspace namespace represents the parent containing the workspace "
              "(the Terra billing project) "
              "If omitted, the CLI configured `workspace_namespace` will be used. ")
    )
}

@workflow_cli.command("list-submissions", arguments={**workspace_args})
def list_submissions(args: argparse.Namespace):
    """
    List workflow submissions in chronological order
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    listing = [(s['submissionId'], s['submissionDate'], s['status'])
               for s in workflows.list_submissions(args.workspace, args.workspace_namespace)]
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
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    submission = workflows.get_submission(args.submission_id, args.workspace, args.workspace_namespace)
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
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    wf = workflows.get_workflow(args.submission_id, args.workflow_id, args.workspace, args.workspace_namespace)
    print(json.dumps(wf, indent=2))

@workflow_cli.command("estimate-submission-cost", arguments={
    "--submission-id": dict(type=str, required=True),
    ** workspace_args
})
def estimate_submission_cost(args: argparse.Namespace):
    """
    Estimate costs for all workflows in a submission
    """
    args.workspace, args.workspace_namespace = CLIConfig.resolve(args.workspace, args.workspace_namespace)
    workflows_metadata = workflows.get_all_workflows(args.submission_id, args.workspace, args.workspace_namespace)
    reporter = TXTReport([("workflow_id", 37),
                          ("shard", 6),
                          ("cpus", 5),
                          ("memory (GB)", 12),
                          ("duration (h)", 13),
                          ("call cached", 12),
                          ("cost", 5)])
    reporter.print_headers()
    total = 0
    for workflow_id, workflow_metadata in workflows_metadata.items():
        shard = 1
        for item in workflows.estimate_workflow_cost(workflow_id, workflow_metadata):
            cost, cpus, mem, duration, call_cached = (item[k] for k in ('cost',
                                                                        'number_of_cpus',
                                                                        'memory',
                                                                        'duration',
                                                                        'call_cached'))
            reporter.print_line(workflow_id, shard, cpus, mem, duration / 3600, call_cached, cost)
            total += cost
            shard += 1
    reporter.print_divider()
    reporter.print_line("", "", "", "", "", "", total)

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
        if isinstance(val, (str, bool)):
            return f"%{width}s" % val
        elif isinstance(val, int):
            return f"%{width}i" % val
        elif isinstance(val, float):
            return f"%{width}.2f" % round(val, decimals)
        else:
            raise TypeError(f"Unsupported type '{type(val)}'")
