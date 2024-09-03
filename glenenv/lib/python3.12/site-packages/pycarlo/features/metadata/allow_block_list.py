import enum
import re
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from dataclasses_json import config, dataclass_json

from pycarlo.common import get_logger

logger = get_logger(__name__)

# For documentation and samples check the link below:
# https://www.notion.so/montecarlodata/Catalog-Schema-Filtering-59edd6eff7f74c94ab6bfca75d2e3ff1


def _exclude_none_values(value: Any) -> bool:
    return value is None


class FilterEffectType(enum.Enum):
    BLOCK = "block"
    ALLOW = "allow"


class FilterType(enum.Enum):
    EXACT_MATCH = "exact_match"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    SUBSTRING = "substring"
    REGEXP = "regexp"


@dataclass_json
@dataclass
class MetadataFilter:
    # we're using exclude=_exclude_none_values to prevent these properties to be serialized to json
    # when None, to keep the json doc simpler
    project: Optional[str] = field(metadata=config(exclude=_exclude_none_values), default=None)
    dataset: Optional[str] = field(metadata=config(exclude=_exclude_none_values), default=None)
    table_type: Optional[str] = field(metadata=config(exclude=_exclude_none_values), default=None)
    table_name: Optional[str] = field(metadata=config(exclude=_exclude_none_values), default=None)
    type: FilterType = FilterType.EXACT_MATCH
    effect: FilterEffectType = FilterEffectType.BLOCK

    def matches(self, force_regexp: bool = False, **kwargs: Any) -> bool:
        """
        Returns True if all properties specified in kwargs match the conditions specified in
        properties of the same name in this object.
        Supported keys in kwargs: 'project', 'dataset', 'table', 'table_type'
        For example kwargs={'project': 'prj_1'} will evaluate if 'prj_1' matches the condition in
        self.project. For kwargs={'project': 'prj_1', 'dataset': 'ds_1'} will evaluate if 'prj_1'
        matches the condition in self.project and if 'ds_1' matches the condition in self.dataset.
        If any of the conditions (for example self.project) is None, that condition will be matched.
        """
        if not kwargs:
            raise ValueError("At least one field needs to be specified for matching")

        # kwargs must match the field names in this class, if any of them do not,
        # invalidate the filter.
        try:
            is_match = all(
                self._safe_match(
                    component=getattr(self, component),
                    value=value,
                    force_regexp=force_regexp,
                    filter_type=self.type
                    if self.filter_type_target_field() == component
                    else FilterType.EXACT_MATCH,
                )
                for component, value in kwargs.items()
            )
        except AttributeError:
            is_match = False

        return is_match

    def filter_type_target_field(self) -> str:
        """
        The field that is evaluated using filter type. Other fields should be
        compared using exact match.
        """
        if self.table_name is not None:
            return "table_name"
        if self.dataset is not None:
            return "dataset"
        if self.project is not None:
            return "project"

        logger.exception("Invalid filter, missing target values")
        return ""

    @classmethod
    def _safe_match(
        cls,
        component: Optional[str],
        value: Optional[str],
        force_regexp: bool,
        filter_type: FilterType,
    ) -> bool:
        # Field not specified on this object, e.g. self.dataset=None, which matches everything
        if component is None:
            return True
        # The value in kwargs is empty, it does not match the condition.
        if value is None:
            return False

        # Convert it in lowercase. In the normalizer we are converting identifiers
        # (like project/dataset) to lowercase so the metadata filters may be defined with
        # lowercase on the UI, however on Snowflake the identifiers are usually in uppercase.
        # Therefore, we perform the evaluation case-insensitive.
        component = component.lower()
        value = value.lower()

        if force_regexp or filter_type == FilterType.REGEXP:
            regexp = f"^{component}$"  # Anchor the regexp to be more strict about what to match.
            return re.match(regexp, value) is not None
        elif filter_type == FilterType.PREFIX:
            return value.startswith(component)
        elif filter_type == FilterType.SUFFIX:
            return value.endswith(component)
        elif filter_type == FilterType.SUBSTRING:
            return component in value
        else:
            return component == value


@dataclass_json
@dataclass
class AllowBlockList:
    filters: List[MetadataFilter] = field(default_factory=list)
    default_effect: FilterEffectType = FilterEffectType.ALLOW

    @property
    def other_effect(self) -> FilterEffectType:
        return (
            FilterEffectType.ALLOW
            if self.default_effect == FilterEffectType.BLOCK
            else FilterEffectType.BLOCK
        )

    def get_default_effect_filters(
        self, condition: Optional[Callable[[MetadataFilter], bool]] = None
    ) -> List[MetadataFilter]:
        return list(
            filter(
                lambda f: f.effect == self.default_effect and (condition is None or condition(f)),
                self.filters,
            )
        )

    def get_other_effect_filters(
        self, condition: Optional[Callable[[MetadataFilter], bool]] = None
    ) -> List[MetadataFilter]:
        return list(
            filter(
                lambda f: f.effect != self.default_effect and (condition is None or condition(f)),
                self.filters,
            )
        )
