from typing import Dict, Optional, cast

from pycarlo.core import Client, Query
from pycarlo.features.pii import PiiFilteringFailModeType
from pycarlo.features.pii.pii_filterer import PiiActiveFilter, PiiActiveFiltersConfig
from pycarlo.features.pii.queries import GET_PII_FILTERS, GET_PII_PREFERENCES


class PiiService:
    def __init__(self, mc_client: Optional[Client] = None):
        self._mc_client = mc_client or Client()

    def get_pii_filters_config(self) -> Optional[Dict]:
        prefs = cast(
            Query, self._mc_client(query=GET_PII_PREFERENCES)
        ).get_pii_filtering_preferences
        if not prefs.enabled:
            return None

        fail_closed = prefs.fail_mode.upper() == PiiFilteringFailModeType.CLOSE
        pii_filters = cast(Query, self._mc_client(query=GET_PII_FILTERS)).get_pii_filters
        if not pii_filters:
            return None

        return PiiActiveFiltersConfig(
            fail_closed=fail_closed,
            active=[
                PiiActiveFilter(
                    name=cast(str, f.name),
                    pattern=cast(str, f.pattern),
                )
                for f in pii_filters
                if f.enabled
            ],
        ).to_dict()  # type: ignore
