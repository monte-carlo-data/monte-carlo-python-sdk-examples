import json
from dataclasses import dataclass
from typing import List, Optional

from dataclasses_json import LetterCase, dataclass_json
from marshmallow import fields

from montecarlodata.iac.utils import field_spec
from montecarlodata.settings import (
    DEFAULT_EXCLUDE_PATTERNS,
    DEFAULT_INCLUDE_PATTERNS,
    DEFAULT_MONTECARLO_MONITOR_CONFIG_VERSION,
)


@dataclass_json
@dataclass
class ProjectConfig:
    version: Optional[int] = field_spec(
        fields.Int(required=False),
        default_factory=lambda: DEFAULT_MONTECARLO_MONITOR_CONFIG_VERSION,
    )
    default_resource: Optional[str] = field_spec(fields.Str(required=False))
    include_file_patterns: Optional[List[str]] = field_spec(
        fields.List(fields.Str(required=True)),
        default_factory=lambda: DEFAULT_INCLUDE_PATTERNS,
    )
    exclude_file_patterns: Optional[List[str]] = field_spec(
        fields.List(fields.Str(required=True)), default_factory=list
    )
    namespace: Optional[str] = None

    def __post_init__(self):
        self.exclude_file_patterns = list(
            set((self.exclude_file_patterns or []) + DEFAULT_EXCLUDE_PATTERNS)
        )


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class ResourceModification:
    type: str
    description: str
    is_significant_change: bool = False
    diff_string: Optional[str] = None
    resource_type: Optional[str] = None
    resource_index: Optional[int] = None


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class ConfigTemplateUpdateAsyncResponse:
    update_uuid: Optional[str] = None
    errors_as_json: Optional[str] = None
    warnings_as_json: Optional[str] = None

    def __post_init__(self):
        self.errors = json.loads(self.errors_as_json) if self.errors_as_json else {}


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class ConfigTemplateUpdateState:
    state: str
    resource_modifications: Optional[List[ResourceModification]] = None
    errors_as_json: Optional[str] = None
    warnings_as_json: Optional[str] = None
    changes_applied: bool = False

    def __post_init__(self):
        self.resource_modifications = self.resource_modifications or []
        self.errors = json.loads(self.errors_as_json) if self.errors_as_json else {}

    @property
    def warnings(self):
        return json.loads(self.warnings_as_json) if self.warnings_as_json else {}


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class ConfigTemplateDeleteResponse:
    num_deleted: int
    changes_applied: bool = False
