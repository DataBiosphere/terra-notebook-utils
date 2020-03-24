import os

WORKSPACE_NAME = os.environ.get('WORKSPACE_NAME', None)
WORKSPACE_GOOGLE_PROJECT = os.environ.get('GCLOUD_PROJECT', None)
# For a list of gcloud project related environment variables, see:
# https://cloud.google.com/functions/docs/env-var#environment_variables_set_automatically
WORKSPACE_BUCKET = os.environ.get('WORKSPACE_BUCKET', None)
DRS_SCHEMA = 'drs://'
GS_SCHEMA = 'gs://'
if WORKSPACE_BUCKET is not None and WORKSPACE_BUCKET.startswith(GS_SCHEMA):
    WORKSPACE_BUCKET = WORKSPACE_BUCKET[len(GS_SCHEMA):]  # Chop off the bucket schema
