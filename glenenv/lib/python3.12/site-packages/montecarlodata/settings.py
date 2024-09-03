import os
from pathlib import Path

"""Environmental configuration"""

# Verbose error logging
MCD_VERBOSE_ERRORS = os.getenv("MCD_VERBOSE_ERRORS", False) in (True, "true", "True")

# MCD API endpoint. Overwrites config-file
MCD_API_ENDPOINT = os.getenv("MCD_API_ENDPOINT")

# Default MCD API endpoint when no env or config file setting is available
MCD_DEFAULT_API_ENDPOINT = "https://api.getmontecarlo.com/graphql"

# MCD ID header (for use in local development and testing)
MCD_USER_ID_HEADER = os.getenv("MCD_USER_ID_HEADER")

# MCD API ID. Overwrites config-file
MCD_DEFAULT_API_ID = os.getenv("MCD_DEFAULT_API_ID")

# MCD API Token. Overwrites config-file
MCD_DEFAULT_API_TOKEN = os.getenv("MCD_DEFAULT_API_TOKEN")

# MCD Agent image host. Overwrites config-file
MCD_AGENT_IMAGE_HOST = os.getenv("MCD_AGENT_IMAGE_HOST")

# Default MCD Agent image host when no env or config file setting is available
MCD_DEFAULT_AGENT_IMAGE_HOST = "docker.io"

# MCD Agent image organization. Overwrites config-file
MCD_AGENT_IMAGE_ORG = os.getenv("MCD_AGENT_IMAGE_ORG")

# Default MCD Agent image organization when no env or config file setting is available
MCD_DEFAULT_AGENT_IMAGE_ORG = "montecarlodata"

# MCD Agent image repository. Overwrites config-file
MCD_AGENT_IMAGE_REPO = os.getenv("MCD_AGENT_IMAGE_REPO")

# Default MCD Agent image repository when no env or config file setting is available
MCD_DEFAULT_AGENT_IMAGE_REPO = "agent"

# dbt cloud API token
DBT_CLOUD_API_TOKEN = os.getenv("DBT_CLOUD_API_TOKEN")

# dbt cloud account ID
DBT_CLOUD_ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")


"""Tool Defaults"""

# Default profile to be used
DEFAULT_PROFILE_NAME = "default"

# Default path where any configuration files are written
DEFAULT_CONFIG_PATH = os.path.join(str(Path.home()), ".mcd")

# Default region where data collector is deployed
DEFAULT_AWS_REGION = "us-east-1"

"""Internal Use"""

# File name for profile configuration
PROFILE_FILE_NAME = "profiles.ini"

# Configuration sub-command
CONFIG_SUB_COMMAND = "configure"

# Help flag of arguments and options
HELP_FLAG = "--help"

# Option file flag
OPTION_FILE_FLAG = "--option-file"

# Value (if entered) to show prompt with hidden input
SHOW_PROMPT_VALUE = "-1"

"""Monitors as code configs"""

DEFAULT_MONTECARLO_MONITOR_CONFIG_VERSION = 1

PROJECT_CONFIG_FILENAME_YML = "montecarlo.yml"
PROJECT_CONFIG_FILENAME_YAML = "montecarlo.yaml"

DBT_CONFIG_YML = "dbt_project.yml"
DBT_CONFIG_YAML = "dbt_project.yaml"

TARGET_DIRECTORY_NAME = "target"

DEFAULT_INCLUDE_PATTERNS = [
    "**/*.yaml",
    "**/*.yml",
]

DEFAULT_EXCLUDE_PATTERNS = [
    "target/*",
    DBT_CONFIG_YML,
    DBT_CONFIG_YAML,
    PROJECT_CONFIG_FILENAME_YML,
    PROJECT_CONFIG_FILENAME_YAML,
]
