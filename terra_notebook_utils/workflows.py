"""
Workflow information
"""
import os
import json
import logging
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Generator, Optional, Tuple

import jmespath
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

def _get(path: str, data: Dict[str, Any]) -> Any:
    res = jmespath.search(path, data)
    if res is None:
        raise TNUCostException(f"'{path}' not found in {json.dumps(data, indent=2)}")
    return res

def estimate_workflow_cost(submission_id: str,
                           workflow_id: str,
                           workspace_name: Optional[str]=WORKSPACE_NAME,
                           workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT):
    workflow_metadata = get_workflow(submission_id, workflow_id, workspace_name, workspace_namespace)
    for call_name, call_metadata_list in workflow_metadata['calls'].items():
        for call_metadata in call_metadata_list:
            try:
                task_name = call_name.split(".")[1]
                call_cached = bool(int(_get("callCaching.hit", call_metadata)))
                if call_cached:
                    cost, cpus, memory_gb, runtime = 0.0, 0, 0.0, 0.0
                else:
                    cpus, memory_gb = _parse_machine_type(_get("jes.machineType", call_metadata))
                    # Assume that Google Lifesciences Pipelines API uses N1 custome machine type
                    start = datetime.strptime(_get("start", call_metadata), date_format)
                    end = datetime.strptime(_get("end", call_metadata), date_format)
                    runtime = (end - start).total_seconds()
                    preemptible = bool(int(_get("runtimeAttributes.preemptible", call_metadata)))
                    cost = costs.GCPCustomN1Cost.estimate(cpus, memory_gb, runtime, preemptible)
                yield dict(task_name=task_name,
                           cost=cost,
                           number_of_cpus=cpus,
                           memory=memory_gb,
                           duration=runtime,
                           call_cached=call_cached)
            except TNUCostException as exc:
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
