import contextlib
from dataclasses import field
from typing import Any, Dict

from box import Box
from dataclasses_json import config


def field_spec(mm_field, default_factory=None, encoder=None, decoder=None):
    """
    dataclasses-json allows configuring a Marshmallow field which will be used for
    schema validation.
    See https://github.com/lidatong/dataclasses-json#overriding--extending
    Syntax involves a lot of boilerplate, so trying to minimize by using this function.
    """
    f = field(metadata=config(mm_field=mm_field, encoder=encoder, decoder=decoder))

    # The following makes all of our dataclass fields optional.
    # We are enforcing required fields using marshmallow validators, so let's simplify and make
    # all the dataclass fields optional.
    if default_factory:
        f.default_factory = default_factory
    else:
        f.default = None

    return f


def is_dbt_schema(yaml_as_dict: Box) -> bool:
    with contextlib.suppress(KeyError):
        return isinstance(yaml_as_dict.models, list)
    return False


def has_montecarlo_property(yaml_as_dict: Dict[str, Any]) -> bool:
    return "montecarlo" in yaml_as_dict


def has_namespace_property(yaml_as_dict: Dict[str, Any]) -> bool:
    return "namespace" in yaml_as_dict
