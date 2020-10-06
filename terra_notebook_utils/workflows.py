"""
Workflow information
"""
import os
from functools import lru_cache
from typing import Generator, Optional

from firecloud import fiss

from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT


def list_submissions(workspace_name: Optional[str]=WORKSPACE_NAME,
                     google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> Generator[dict, None, None]:
    """
    List all submissions in the workspace `workspace_name`.
    """
    resp = fiss.fapi.list_submissions(google_billing_project, workspace_name)
    resp.raise_for_status()
    for s in resp.json():
        yield s

@lru_cache()
def get_submission(submission_id: str,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> dict:
    """
    Get information about a submission, including member workflows
    """
    resp = fiss.fapi.get_submission(google_billing_project, workspace_name, submission_id)
    resp.raise_for_status()
    return resp.json()

@lru_cache()
def get_workflow(submission_id: str,
                 workflow_id: str,
                 workspace_name: Optional[str]=WORKSPACE_NAME,
                 google_billing_project: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> dict:
    """
    Get information about a workflow
    """
    resp = fiss.fapi.get_workflow_metadata(google_billing_project, workspace_name, submission_id, workflow_id)
    resp.raise_for_status()
    return resp.json()
