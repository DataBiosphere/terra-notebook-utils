import os

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def get_service(storage_account_name: str):
    if os.environ.get("TERRA_NOTEBOOK_AZURE_ACCESS_KEY"):
        print("using access key")
        access_key = os.environ.get("TERRA_NOTEBOOK_AZURE_ACCESS_KEY")
        connection_string = (f"DefaultEndpointsProtocol=https;AccountName="
                             f"{storage_account_name};AccountKey={access_key};EndpointSuffix=core.windows.net")
        service = BlobServiceClient.from_connection_string(connection_string)
    else:
        print("using DefaultAzureCredential")
        default_credential = DefaultAzureCredential(logging_enable=True)
        service = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net/",
                                    credential=default_credential)
    return service

client = get_service("qijlbdgpc4zqdee").get_blob_client("qi-test-container", "qi-blob-1")
with open("./README.md", "rb") as data:
    client.upload_blob(data)

# with open("test", "wb") as data:
#     data.write(client.download_blob().readall())
