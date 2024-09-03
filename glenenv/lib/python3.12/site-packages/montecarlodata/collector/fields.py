# Query responses
EXPECTED_GENERATE_TEMPLATE_GQL_RESPONSE_FIELD = "generateCollectorTemplate"
EXPECTED_TTC_RESPONSE_FIELD = "testTelnetConnection"
EXPECTED_TTOC_RESPONSE_FIELD = "testTcpOpenConnection"
EXPECTED_ADD_DC_RESPONSE_FIELD = "createCollectorRecord"

DEFAULT_COLLECTION_REGION = "us-east-1"

# Verbiage
ADD_DC_REGION_PROMPT_VERBIAGE = "Which AWS region would you like to deploy in?"
ADD_DC_PROMPT_VERBIAGE = (
    "Do you want deploy this collector right now? "
    "This opens a browser to CF console with a quick create link"
)
