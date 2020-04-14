import os

os.environ['GCLOUD_PROJECT'] = "firecloud-cgl"
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"

cred_data = os.environ.get("TNU_GCP_SERVICE_ACCOUNT_CREDENTIALS_DATA")
if cred_data:
    import base64
    with open("creds.json", "wb") as fh:
        fh.write(base64.b64decode(cred_data))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "creds.json"
