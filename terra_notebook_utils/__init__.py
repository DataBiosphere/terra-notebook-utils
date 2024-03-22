import os
from dataclasses import dataclass
from enum import Enum

WORKSPACE_NAME = os.environ.get('WORKSPACE_NAME', None)
WORKSPACE_NAMESPACE = os.environ.get('WORKSPACE_NAMESPACE')  # This env var is set in Terra Cloud Environments
WORKSPACE_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT')  # This env var is set in Terra Cloud Environments
TERRA_DEPLOYMENT_ENV = os.environ.get('TERRA_DEPLOYMENT_ENV', 'prod')
DRS_RESOLVER_ENDPOINT = os.environ.get("DRS_RESOLVER_ENDPOINT", 'api/v4/drs/resolve')

if not WORKSPACE_GOOGLE_PROJECT:
    WORKSPACE_GOOGLE_PROJECT = os.environ.get('GCP_PROJECT')  # Useful for running outside of notebook
if not WORKSPACE_GOOGLE_PROJECT:
    WORKSPACE_GOOGLE_PROJECT = os.environ.get('GCLOUD_PROJECT')  # Fallback
# For a list of gcloud project related environment variables, see:
# https://cloud.google.com/functions/docs/env-var#environment_variables_set_automatically
WORKSPACE_BUCKET = os.environ.get('WORKSPACE_BUCKET', None)
_GS_SCHEMA = 'gs://'
if WORKSPACE_BUCKET is not None and WORKSPACE_BUCKET.startswith(_GS_SCHEMA):
    WORKSPACE_BUCKET = WORKSPACE_BUCKET[len(_GS_SCHEMA):]  # Chop off the bucket schema

IO_CONCURRENCY = 3

DRS_RESOLVER_URL = f"https://drshub.dsde-{TERRA_DEPLOYMENT_ENV}.broadinstitute.org/{DRS_RESOLVER_ENDPOINT}"

class ExecutionEnvironment(Enum):
    TERRA_WORKSPACE = "TERRA_WORKSPACE",  # Executing in a Terra Workspace (on any supported platform)
    OTHER = "OTHER"  # Executing outside a Terra Workspace (e.g., local system)


class ExecutionPlatform(Enum):
    AZURE = "AZURE",  # Executing in an Azure compute environment
    GOOGLE = "GOOGLE",  # Executing in a Google compute environment
    UNKNOWN = "UNKNOWN"  # Execution platform not identified (e.g., local system)


@dataclass
class ExecutionContext:
    execution_environment: ExecutionEnvironment
    execution_platform: ExecutionPlatform
