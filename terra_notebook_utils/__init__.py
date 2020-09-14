import os
import json

from typing import Optional


class Config:
    info = dict(workspace=None, workspace_google_project=None)
    path = os.path.join(os.path.expanduser("~"), ".tnu_config")

    @classmethod
    def load(cls):
        if not os.path.isfile(cls.path):
            cls.write()
        with open(cls.path) as fh:
            cls.info = json.loads(fh.read())

    @classmethod
    def write(cls):
        with open(cls.path, "w") as fh:
            fh.write(json.dumps(cls.info, indent=2))

    @classmethod
    def resolve(cls, override_workspace: Optional[str] = None, override_namespace: Optional[str] = None):
        workspace_name = (override_workspace or
                          cls.info['workspace'] or
                          os.environ.get('WORKSPACE_NAME'))
        namespace = (override_namespace or
                     cls.info['workspace_google_project'] or
                     os.environ.get('GOOGLE_PROJECT') or  # This env var is set in Terra notebooks
                     os.environ.get('GCP_PROJECT') or  # Useful for running outside of notebook
                     os.environ.get('GCLOUD_PROJECT'))  # Fallback
        if workspace_name and namespace is None:
            from terra_notebook_utils.workspace import get_workspace_namespace
            namespace = get_workspace_namespace(workspace_name)
        if not workspace_name:
            raise RuntimeError("This command requires a workspace. Either pass in a workspace with `--workspace`,"
                               " or configure a default workspace for the cli (see `tnu config --help`)."
                               " A default workspace may also be configured by setting the `WORKSPACE_NAME` env var")
        return workspace_name, namespace


Config.load()
WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT = Config.resolve()
TERRA_DEPLOYMENT_ENV = os.environ.get('TERRA_DEPLOYMENT_ENV', 'prod')

# For a list of gcloud project related environment variables, see:
# https://cloud.google.com/functions/docs/env-var#environment_variables_set_automatically
WORKSPACE_BUCKET = os.environ.get('WORKSPACE_BUCKET', None)
_GS_SCHEMA = 'gs://'
if WORKSPACE_BUCKET is not None and WORKSPACE_BUCKET.startswith(_GS_SCHEMA):
    WORKSPACE_BUCKET = WORKSPACE_BUCKET[len(_GS_SCHEMA):]  # Chop off the bucket schema

MULTIPART_THRESHOLD = 1024 * 1024 * 32
IO_CONCURRENCY = 3

from terra_notebook_utils import drs, profile, table, vcf, workspace
