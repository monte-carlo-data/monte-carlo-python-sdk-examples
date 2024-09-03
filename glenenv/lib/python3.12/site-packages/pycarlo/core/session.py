import configparser
import os
import uuid
from dataclasses import InitVar, dataclass, field
from typing import Optional

import pkg_resources

from pycarlo.common import get_logger
from pycarlo.common.errors import InvalidConfigFileError, InvalidSessionError
from pycarlo.common.settings import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_MCD_API_ENDPOINT,
    DEFAULT_MCD_API_ENDPOINT_CONFIG_KEY,
    DEFAULT_MCD_API_ID_CONFIG_KEY,
    DEFAULT_MCD_API_TOKEN_CONFIG_KEY,
    DEFAULT_MCD_IGW_ENDPOINT,
    DEFAULT_PACKAGE_NAME,
    DEFAULT_PROFILE_NAME,
    MCD_API_ENDPOINT,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    MCD_DEFAULT_PROFILE,
    MCD_USER_ID_HEADER,
    PROFILE_FILE_NAME,
)

logger = get_logger(__name__)


@dataclass
class Session:
    """
    Creates an MC access session.

    Auth resolution hierarchy -
    1. Passing credentials (mcd_id & mcd_token)
    2. Environment variables (MCD_DEFAULT_API_ID & MCD_DEFAULT_API_TOKEN)
    3. Config-file by passing passing profile name (mcd_profile)
    4. Config-file by setting the profile as an environment variable (MCD_DEFAULT_PROFILE)
    5. Config-file by default profile name (default)

    Environment vars can be mixed with passed credentials, but not the config-file profile.

    If necessary the MC API url can be overridden by specifying an endpoint.

    The config-file path can be set via mcd_config_path.

    An optional scope can be set to configure the Session to use the Integration Gateway
    REST API instead of the GraphQL API.
    """

    mcd_id: InitVar[Optional[str]] = None
    mcd_token: InitVar[Optional[str]] = None
    mcd_profile: InitVar[Optional[str]] = None
    mcd_config_path: InitVar[str] = DEFAULT_CONFIG_PATH

    id: str = field(init=False)
    token: str = field(init=False)
    session_name: str = field(init=False)
    endpoint: str = DEFAULT_MCD_API_ENDPOINT
    user_id: Optional[str] = MCD_USER_ID_HEADER
    scope: Optional[str] = None

    def __post_init__(
        self,
        mcd_id: Optional[str],
        mcd_token: Optional[str],
        mcd_profile: Optional[str],
        mcd_config_path: str,
    ):
        version = pkg_resources.get_distribution(DEFAULT_PACKAGE_NAME).version
        self.session_name = f"python-sdk-{version}-{uuid.uuid4()}"
        logger.info(f"Creating named session as '{self.session_name}'.")

        mcd_id = mcd_id or MCD_DEFAULT_API_ID
        mcd_token = mcd_token or MCD_DEFAULT_API_TOKEN
        if mcd_id and mcd_token:
            self.id = mcd_id
            self.token = mcd_token
        elif mcd_id or mcd_token:
            raise InvalidSessionError("Partially setting a session is not supported.")
        else:
            self._read_config(
                mcd_profile=mcd_profile or MCD_DEFAULT_PROFILE or DEFAULT_PROFILE_NAME,
                mcd_config_path=mcd_config_path,
            )

        if MCD_API_ENDPOINT:
            self.endpoint = MCD_API_ENDPOINT
        elif self.scope and self.endpoint == DEFAULT_MCD_API_ENDPOINT:
            # if scope is set and endpoint is the default one, change it to IGW
            self.endpoint = DEFAULT_MCD_IGW_ENDPOINT

        session_type = "GATEWAY_API" if self.scope else "APPLICATION_API"
        logger.info(f"Created {session_type} session with MC API ID '{self.id}'.")

    def _read_config(self, mcd_profile: str, mcd_config_path: str) -> None:
        """
        Return configuration from section (profile name) if it exists.
        """
        config_parser = Session._get_config_parser()
        file_path = os.path.join(mcd_config_path, PROFILE_FILE_NAME)
        logger.info(
            "No provided connection details. Looking up session values from "
            f"'{mcd_profile}' in '{file_path}'."
        )

        try:
            config_parser.read(file_path)
            self.id = config_parser.get(mcd_profile, DEFAULT_MCD_API_ID_CONFIG_KEY)
            self.token = config_parser.get(mcd_profile, DEFAULT_MCD_API_TOKEN_CONFIG_KEY)
            self.endpoint = config_parser.get(
                mcd_profile, DEFAULT_MCD_API_ENDPOINT_CONFIG_KEY, fallback=DEFAULT_MCD_API_ENDPOINT
            )
        except configparser.NoSectionError:
            raise InvalidSessionError(f"Profile '{mcd_profile}' not found in '{file_path}'.")
        except Exception as err:
            raise InvalidConfigFileError from err

    @staticmethod
    def _get_config_parser() -> configparser.ConfigParser:
        """
        Gets a configparser
        """
        return configparser.ConfigParser()
