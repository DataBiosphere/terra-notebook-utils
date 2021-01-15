"""
Workflow information
"""
import json
import logging
from datetime import datetime
from functools import lru_cache
from typing import Dict, Generator, Optional, Tuple

from firecloud import fiss

from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, costs
from terra_notebook_utils.utils import concurrent_recursion, js_get


logger = logging.getLogger(__name__)

date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

class TNUCostException(Exception):
    pass

def list_submissions(workspace_name: Optional[str]=WORKSPACE_NAME,
                     workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> Generator[dict, None, None]:
    resp = fiss.fapi.list_submissions(workspace_namespace, workspace_name)
    resp.raise_for_status()
    for s in resp.json():
        yield s

@lru_cache()
def get_submission(submission_id: str,
                   workspace_name: Optional[str]=WORKSPACE_NAME,
                   workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> dict:
    """
    Get information about a submission, including member workflows
    """
    resp = fiss.fapi.get_submission(workspace_namespace, workspace_name, submission_id)
    resp.raise_for_status()
    return resp.json()

@lru_cache()
def get_workflow(submission_id: str,
                 workflow_id: str,
                 workspace_name: Optional[str]=WORKSPACE_NAME,
                 workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> dict:
    """
    Get information about a workflow
    """
    resp = fiss.fapi.get_workflow_metadata(workspace_namespace, workspace_name, submission_id, workflow_id)
    resp.raise_for_status()
    return resp.json()

def get_all_workflows(submission_id: str,
                      workspace: Optional[str]=WORKSPACE_NAME,
                      workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> Dict[str, dict]:
    """
    Retrieve all workflows, and workflow metadata, for `submission_id`, including sub-workflows.
    """
    workflows_metadata = dict()

    def get_metadata_and_subworkflows(workflow_id: str):
        wf_medadata = get_workflow(submission_id, workflow_id, workspace, workspace_namespace)
        workflows_metadata[workflow_id] = wf_medadata
        subworkflows = {call_metadata['subWorkflowId']
                        for call_metadata_list in wf_medadata['calls'].values()
                        for call_metadata in call_metadata_list
                        if "subWorkflowId" in call_metadata}
        return subworkflows

    submission = get_submission(submission_id, workspace, workspace_namespace)
    initial_workflow_ids = {wf['workflowId'] for wf in submission['workflows']}
    concurrent_recursion(get_metadata_and_subworkflows, initial_workflow_ids)

    return workflows_metadata

def estimate_workflow_cost(workflow_id: str, workflow_metadata: dict) -> Generator[dict, None, None]:
    for call_name, call_metadata_list in workflow_metadata['calls'].items():
        for call_metadata in call_metadata_list:
            if "subWorkflowId" in call_metadata:
                # subworkflows need to be looked up and estimated separately
                continue
            try:
                task_name = call_name.split(".")[1]
                call_cached = bool(int(js_get("callCaching.hit", call_metadata)))
                if call_cached:
                    cost, cpus, memory_gb, runtime = 0.0, 0, 0.0, 0.0
                else:
                    cpus, memory_gb = _parse_machine_type(js_get("jes.machineType", call_metadata))
                    # Assume that Google Lifesciences Pipelines API uses N1 custome machine type
                    start = datetime.strptime(js_get("start", call_metadata), date_format)
                    end = datetime.strptime(js_get("end", call_metadata), date_format)
                    runtime = (end - start).total_seconds()
                    preemptible = bool(int(js_get("runtimeAttributes.preemptible", call_metadata)))
                    cost = costs.GCPCustomN1Cost.estimate(cpus, memory_gb, runtime, preemptible)
                yield dict(task_name=task_name,
                           cost=cost,
                           number_of_cpus=cpus,
                           memory=memory_gb,
                           duration=runtime,
                           call_cached=call_cached)
            except (KeyError, TNUCostException) as exc:
                logger.warning(f"Unable to estimate costs for workflow {workflow_id}: "
                               f"{exc.args[0]}")

def _parse_machine_type(machine_type: str) -> Tuple[int, float]:
    parts = machine_type.split("-", 2)
    if 3 != len(parts) or "custom" != parts[0]:
        raise TNUCostException(f"Cannot estimate costs for machine type '{machine_type}'"
                               "Please contact terra-notebook-utils maintainers to add support")
    try:
        cpus, memory_gb = int(parts[1]), float(parts[2]) / 1024
        return cpus, memory_gb
    except ValueError as exc:
        raise TNUCostException(f"Cannot parse cpus and memory from '{machine_type}'") from exc
