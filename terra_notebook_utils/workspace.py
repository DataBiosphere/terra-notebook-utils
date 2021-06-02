"""Workspace information and operations."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from firecloud import fiss

from terra_notebook_utils import gs, WORKSPACE_BUCKET, WORKSPACE_NAME


def get_workspace(workspace: Optional[str]=WORKSPACE_NAME, namespace: Optional[str]=None) -> dict:
    namespace = namespace or get_workspace_namespace(workspace)
    resp = fiss.fapi.get_workspace(namespace, workspace)
    resp.raise_for_status()
    return resp.json()

def list_workspaces() -> List[dict]:
    """List workspaces available to current user."""
    resp = fiss.fapi.list_workspaces()
    resp.raise_for_status()
    return resp.json()

def get_workspace_bucket(workspace: Optional[str]=WORKSPACE_NAME) -> Optional[str]:
    """Get Google Storage bucket associated with a workspace."""
    if not workspace:
        raise ValueError("Expected workspace")
    else:
        for ws in list_workspaces():
            if ws['workspace']['name'] == workspace:
                return ws['workspace']['bucketName']
        return None

def get_workspace_namespace(workspace: Optional[str]=WORKSPACE_NAME) -> Optional[str]:
    """Best effort discovery of workspace namespace. If two namespaces share a workspace of the same name, the first
    namespace encountered will be returned.
    """
    if not workspace:
        raise ValueError("Expected workspace")
    else:
        for ws in list_workspaces():
            if ws['workspace']['name'] == workspace:
                return ws['workspace']['namespace']
        return None

def remove_workflow_logs(bucket_name=WORKSPACE_BUCKET, submission_id: str=None) -> List[str]:
    """Experimental: do not use."""
    bucket = gs.get_client().bucket(bucket_name)

    def _is_workflow_log(blob):
        fname = blob.name.rsplit("/", 1)[-1]
        return fname.startswith("workflow") and fname.endswith(".log")

    def _delete_blob(blob):
        blob.delete()
        return blob.name

    prefixes = {blob.name.split("/", 1)[0] for blob in bucket.list_blobs()
                if _is_workflow_log(blob)}
    if submission_id is not None:
        prefixes = {pfx for pfx in prefixes
                    if pfx == submission_id}
    for pfx in prefixes:
        blobs_to_delete = [blob for blob in bucket.list_blobs(prefix=pfx)]
        print(f"Deleting {len(blobs_to_delete)} objects for {pfx}")
        with ThreadPoolExecutor(max_workers=8) as e:
            futures = [e.submit(_delete_blob, blob) for blob in blobs_to_delete]
            deleted_manifest = [f.result() for f in as_completed(futures)]

    return deleted_manifest
