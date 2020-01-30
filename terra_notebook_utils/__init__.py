import os

WORKSPACE_NAME = os.environ.get('WORKSPACE_NAME', None)
WORKSPACE_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT', None)
WORKSPACE_BUCKET = os.environ.get('WORKSPACE_BUCKET', None)
if WORKSPACE_BUCKET is not None:
    WORKSPACE_BUCKET = WORKSPACE_BUCKET[5:]  # Chop off the bucket schema, "gs://"
