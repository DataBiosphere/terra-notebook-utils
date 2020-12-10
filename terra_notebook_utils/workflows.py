"""
Workflow information
"""
import os
import json
import logging
from datetime import datetime
from functools import lru_cache
from typing import Tuple, Generator, Optional

from firecloud import fiss

from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT, costs


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

def estimate_workflow_cost(submission_id: str,
                           workflow_id: str,
                           workspace_name: Optional[str]=WORKSPACE_NAME,
                           workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    all_metadata = get_workflow(submission_id, workflow_id, workspace_name, workspace_namespace)
    machine_type: Optional[str]
    for workflow_name, workflow_metadata in all_metadata['calls'].items():
        for execution_metadata in workflow_metadata:
            try:
                task_name = workflow_name.split(".")[1]
                cpus, memory_mb = _parse_machine_type(execution_metadata)
                memory_gb = int(memory_mb / 1024)
                runtime_hours = _parse_runtime_seconds(execution_metadata)
                # Assume that Google Lifesciences Pipelines API uses N1 custome machine type
                cost = costs.GCPCustomN1Cost.estimate(cpus,
                                                      memory_gb,
                                                      runtime_hours,
                                                      _parse_preemptible(execution_metadata))
                yield dict(task_name=task_name, cost=cost, number_of_cpus=cpus, memory=memory_gb, duration=runtime_hours)
            except TNUCostException as exc:
                logger.warning(f"Unable to estimate costs for workflow {workflow_id}: "
                               f"{exc.args[0]}")

def _catch_key_error(func):
    def _wrapper(execution_metadata: dict):
        try:
            return func(execution_metadata)
        except KeyError as ke:
            missing_key = ke.args[0]
            raise TNUCostException(f"'{func.__name__}' failed: '{missing_key}' not found in workflow metadata")
    return _wrapper

@_catch_key_error
def _parse_runtime_seconds(execution_metadata: dict) -> float:
    start = datetime.strptime(execution_metadata['start'], date_format)
    end = datetime.strptime(execution_metadata['end'], date_format)
    return (end - start).total_seconds()

@_catch_key_error
def _parse_machine_type(execution_metadata: dict) -> Tuple[int, int]:
    machine_type = execution_metadata['jes']['machineType']
    parts = machine_type.split("-", 2)
    if 3 != len(parts) or "custom" != parts[0]:
        raise TNUCostException(f"Cannot estimate costs for machine type '{machine_type}'"
                               "Please contact terra-notebook-utils maintainers to add support")
    try:
        cpus, memory_mb = int(parts[1]), int(parts[2])
        return cpus, memory_mb
    except ValueError as exc:
        raise TNUCostException(f"Cannot parse cpus and memory from '{machine_type}'") from exc

@_catch_key_error
def _parse_preemptible(execution_metadata: dict) -> bool:
    return bool(int(execution_metadata['runtimeAttributes']['preemptible']))
