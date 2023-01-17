"""
Microsoft Azure specific code.
Currently limited to Azure auth.

See:
https://azuresdkdocs.blob.core.windows.net/$web/python/azure-identity/1.12.0/index.html
https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/azure/identity/_credentials/default.py
"""

import os
from typing import Optional

from azure.identity import DefaultAzureCredential
from terra_notebook_utils.logger import logger


# Single instance of DefaultAzureCredential that initialized lazily.
# The instance is treated as threadsafe and reusable.
# The Azure documentation is silent on thread safety.
# Based on scanning the code, it appears to be threadsafe.
# See: https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/
#              azure-identity/azure/identity/_credentials/default.py
_AZURE_CREDENTIAL: Optional[DefaultAzureCredential] = None


def _get_default_credential() -> DefaultAzureCredential:
    """
    Instantiate DefaultAzureCredential lazily if/when needed.

    Note: It would not need to be instantiated this way, as
    # no exception is raised even if Azure credentials are not configured.
    :return: Reference to instance of DefaultAzureCredential
    """

    # Should a more sophisticated Singleton pattern be used instead?
    global _AZURE_CREDENTIAL
    if not _AZURE_CREDENTIAL:
        _AZURE_CREDENTIAL = DefaultAzureCredential()
    return _AZURE_CREDENTIAL


def get_azure_access_token() -> str:
    """
    Return an Azure access token.

    raises ClientAuthenticationError
    """
    if os.environ.get('TERRA_NOTEBOOK_AZURE_ACCESS_TOKEN'):
        logger.debug("Using Azure token configured using 'TERRA_NOTEBOOK_AZURE_ACCESS_TOKEN'")
        token = os.environ['TERRA_NOTEBOOK_AZURE_ACCESS_TOKEN']
    else:
        logger.debug("Requesting Azure default credentials token.")
        token_scope = "https://management.azure.com/.default"
        azure_token = _get_default_credential().get_token(token_scope)
        logger.debug("Using Azure default credentials token.")
        token = azure_token.token
    return token
