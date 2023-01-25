"""
Support for auth with Terra backend services.
"""
from azure.core.exceptions import ClientAuthenticationError
from google.auth.exceptions import DefaultCredentialsError

from terra_notebook_utils import azure_auth, gs, ExecutionPlatform
from terra_notebook_utils.logger import logger
from terra_notebook_utils.utils import get_execution_context


class AuthenticationError(Exception):
    pass


class TerraAuthTokenProvider:
    """
    Provides auth bearer tokens suitable for use with Terra backend services.
    """
    def __init__(self):
        self.execution_context = get_execution_context()

    @staticmethod
    def _identify_valid_access_token() -> str:
        """
        Try to obtain an auth bearer token suitable for use with Terra backend services
        from the Terra supported auth providers. First try Google, then try Azure.
        Return the first successfully obtained token, otherwise raise AuthenticationError.

        :return: auth bearer token suitable for use with Terra backend services
        :raises: AuthenticationError
        """
        try:
            logger.debug("Attempting to obtain a Google access token to use with Terra backend services.")
            google_token = gs.get_access_token()
            logger.debug("Using Google access token to use with Terra backend services.")
            return google_token
        except DefaultCredentialsError as ex:
            logger.debug("Failed to obtain a Google access token to use with Terra backend services.", exc_info=ex)

        try:
            logger.debug("Attempting to obtain a Azure access token to use with Terra backend services.")
            azure_token = azure_auth.get_azure_access_token()
            logger.debug("Using Azure access token to use with Terra backend services.")
            return azure_token
        except ClientAuthenticationError as ex:
            logger.debug("Failed to obtain a Azure access token to use with Terra backend services.", exc_info=ex)

        raise AuthenticationError("Failed to obtain a Google or Azure token to auth with Terra backend services.")

    def get_terra_access_token(self) -> str:
        if self.execution_context.execution_platform == ExecutionPlatform.GOOGLE:
            logger.debug("Using Google default credentials to auth with Terra services.")
            return gs.get_access_token()
        elif self.execution_context.execution_platform == ExecutionPlatform.AZURE:
            logger.debug("Using Azure default credentials to auth with Terra services.")
            return azure_auth.get_azure_access_token()
        else:
            return self._identify_valid_access_token()


# Single instance of TerraAuthTokenProvider.
TERRA_AUTH_TOKEN_PROVIDER = TerraAuthTokenProvider()


def get_terra_access_token() -> str:
    """ Return an auth bearer token suitable for use with Terra backend services.
    :raises: AuthenticationError
    """
    return TERRA_AUTH_TOKEN_PROVIDER.get_terra_access_token()
