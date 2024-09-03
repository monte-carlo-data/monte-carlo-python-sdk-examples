import os
from pathlib import Path

"""Environmental configuration"""

# Enable error logging.
MCD_VERBOSE_ERRORS = os.getenv("MCD_VERBOSE_ERRORS", False) in (True, "true", "True")

# MCD API endpoint.
MCD_API_ENDPOINT = os.getenv("MCD_API_ENDPOINT")

# Override MCD Default Profile when reading from the config-file in a session.
MCD_DEFAULT_PROFILE = os.getenv("MCD_DEFAULT_PROFILE")

# Override MCD API ID when creating a session.
MCD_DEFAULT_API_ID = os.getenv("MCD_DEFAULT_API_ID")

# Override MCD API Token when creating a session.
MCD_DEFAULT_API_TOKEN = os.getenv("MCD_DEFAULT_API_TOKEN")

# MCD ID header (for use in local development and testing)
MCD_USER_ID_HEADER = os.getenv("MCD_USER_ID_HEADER")

# dbt cloud API token
DBT_CLOUD_API_TOKEN = os.getenv("DBT_CLOUD_API_TOKEN")

# dbt cloud account ID
DBT_CLOUD_ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")

"""Internal Use"""

# Default API endpoint when not provided through env variable nor profile
DEFAULT_MCD_API_ENDPOINT = "https://api.getmontecarlo.com/graphql"

# Default Gateway endpoint used when no endpoint is provided through env var or profile
DEFAULT_MCD_IGW_ENDPOINT = "https://integrations.getmontecarlo.com"

# Name of the current package.
DEFAULT_PACKAGE_NAME = "pycarlo"

# Default config keys for the MC config file. Created via the CLI.
DEFAULT_MCD_API_ID_CONFIG_KEY = "mcd_id"
DEFAULT_MCD_API_TOKEN_CONFIG_KEY = "mcd_token"
DEFAULT_MCD_API_ENDPOINT_CONFIG_KEY = "mcd_api_endpoint"

# Default headers for the MC API.
DEFAULT_MCD_API_ID_HEADER = f'x-{DEFAULT_MCD_API_ID_CONFIG_KEY.replace("_", "-")}'
DEFAULT_MCD_API_TOKEN_HEADER = f'x-{DEFAULT_MCD_API_TOKEN_CONFIG_KEY.replace("_", "-")}'
DEFAULT_MCD_USER_ID_HEADER = "user-id"

# Default headers to trace and help identify requests. For debugging.
DEFAULT_MCD_SESSION_ID = "x-mcd-session-id"  # Generally the session name.
DEFAULT_MCD_TRACE_ID = "x-mcd-trace-id"

# File name for profile configuration.
PROFILE_FILE_NAME = "profiles.ini"

# Default profile to be used.
DEFAULT_PROFILE_NAME = "default"

# Default path where any configuration files are written.
DEFAULT_CONFIG_PATH = os.path.join(str(Path.home()), ".mcd")

# Default initial wait time for retries in seconds.
DEFAULT_RETRY_INITIAL_WAIT_TIME = 0.25

# Default maximum wait time for retries in seconds.
DEFAULT_RETRY_MAX_WAIT_TIME = 10.0

# Default initial wait time for idempotent request retries in seconds.
DEFAULT_IDEMPOTENT_RETRY_INITIAL_WAIT_TIME = 4.0

# Default maximum wait time for idempotent request retries in seconds.
DEFAULT_IDEMPOTENT_RETRY_MAX_WAIT_TIME = 4 * pow(
    2, 4
)  # retry 4 times, max wait 64 seconds, total wait 124

# Default timeout for requests sent to Integration Gateway
DEFAULT_IGW_TIMEOUT_SECS = 10
