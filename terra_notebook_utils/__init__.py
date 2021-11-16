import os

WORKSPACE_NAME = os.environ.get('WORKSPACE_NAME', None)
WORKSPACE_NAMESPACE = os.environ.get('WORKSPACE_NAMESPACE')  # This env var is set in Terra Cloud Environments
WORKSPACE_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT')  # This env var is set in Terra Cloud Environments
TERRA_DEPLOYMENT_ENV = os.environ.get('TERRA_DEPLOYMENT_ENV', 'prod')
MARTHA_URL_VERSION = os.environ.get('MARTHA_URL_VERSION', 'martha_v3')

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

MARTHA_URL = f"https://us-central1-broad-dsde-{TERRA_DEPLOYMENT_ENV}.cloudfunctions.net/{MARTHA_URL_VERSION}"
