from terra_notebook_utils.blobstore import url
from typing import Container
from terra_notebook_utils import blobstore
from azure.storage.blob import BlobServiceClient
import os
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from terra_notebook_utils.logger import logger

class AzureBlobStore(blobstore.BlobStore):
    schema = "https://"
    def __init__(self,
                 storage_account,
                 container_name):
        self.storage_account = storage_account
        self.container_name = container_name

    def blob(self, key: str) -> "AzureBlob":
        return AzureBlob(self.storage_account, self.container_name, key)

class AzureBlob(blobstore.Blob):
    def __init__(self,
                storage_account: str,
                container_name: str,
                blob_name: str):
        self.storage_account = storage_account
        self.container_name = container_name
        self.blob_name = blob_name

    @property
    def url(self) -> str:
        return f"https://{self.storage_account}.blob.core.windows.net/{self.container_name}/{self.blob_name}"    

    @property
    def _azure_blob_client(self):
        if not getattr(self, "_container", None):
            if os.environ.get("TERRA_NOTEBOOK_AZURE_ACCESS_KEY"):
                logger.info("Using Access Key")
                access_key = os.environ.get("TERRA_NOTEBOOK_AZURE_ACCESS_KEY")
                connection_str = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account};AccountKey=#{access_key}"
                self._blob_client = BlobServiceClient.from_connection_string(connection_str).get_blob_client(self.container_name, self.blob_name)
            else:
                logger.info("Using DefaultAzureCredential")
                token_credential = DefaultAzureCredential(logging_enable=True)
                self._blob_client = BlobServiceClient(
                    account_url=f"https://{self.storage_account}.blob.core.windows.net",
                    credential=token_credential
                ).get_blob_client(self.container_name, self.blob_name)
        return self._blob_client

    def put(self, data: bytes):
        self._azure_blob_client.upload_blob(data, logging_enable=True)

    def get(self) -> bytes:
        try:
            blob = self._azure_blob_client.download_blob().readall()
        except ResourceNotFoundError:
            raise blobstore.BlobNotFoundError(f"Could not find {url}")
        return blob


    def cloud_native_checksum(self) -> str:
        blob = self._azure_blob_client.get_blob_properties()
        return blob.content_settings.content_md5

    def size(self) -> int:
        try:
            blob = self._azure_blob_client.get_blob_properties()
        except ResourceNotFoundError:
            raise blobstore.BlobNotFoundError(f"Could not find {url}")
        return blob.size

    def delete(self):
        self._azure_blob_client.delete_blob("include")

    def exists(self):
        return self._azure_blob_client.exists()
