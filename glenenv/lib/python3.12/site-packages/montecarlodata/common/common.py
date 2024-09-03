import base64
import ipaddress
import json
import pkgutil
import secrets
from collections.abc import Mapping, MutableMapping
from functools import wraps
from typing import Any, Callable, Dict, Iterator, List, Optional

from box import Box
from jinja2 import Template
from pycarlo.core import Client, Session

from montecarlodata.config import Config


class ConditionalDictionary(MutableMapping):
    """
    A dictionary where you can specify a condition
    when initially assigning a value (e.g. no null values)
    """

    def __init__(self, condition: Callable):
        self.condition = condition
        self.mapping = {}

    def __setitem__(self, key, value) -> None:
        if key in self.mapping or self.condition(value):
            self.mapping[key] = value

    def __delitem__(self, key) -> None:
        del self.mapping[key]

    def __getitem__(self, key):
        return self.mapping[key]

    def __len__(self) -> int:
        return len(self.mapping)

    def __iter__(self) -> Iterator:
        return iter(self.mapping)


def normalize_gql(field: str) -> Optional[str]:
    if field:
        return field.replace("_", "-").lower()


def read_as_base64(path: str) -> bytes:
    with open(path, "rb") as fp:
        return base64.b64encode(fp.read())


def read_as_json(path: str) -> Dict:
    with open(path) as file:
        return json.load(file)


def read_as_json_string(path: str) -> str:
    """ "Read and validate JSON file"""
    return json.dumps(read_as_json(path))


def struct_match(s1: Dict, s2: Dict) -> bool:
    return json.dumps(s1, sort_keys=True) == json.dumps(s2, sort_keys=True)


def boxify(
    use_snakes: bool = False,
    default_box_attr: Optional[Any] = object(),
    default_box: bool = False,
):
    """
    Convenience decorator to convert a dict into Box for ease of use.

    Set `use_snakes` to convert camelCase to snake_case. Use `default_box_attr`
    to set a default value.
    """

    def _boxify(func):
        @wraps(func)
        def _impl(self, *args, **kwargs):
            dict_ = func(self, *args, **kwargs)
            if dict_ and isinstance(dict_, Mapping):
                return Box(
                    dict_,
                    camel_killer_box=use_snakes,
                    default_box_attr=default_box_attr,
                    default_box=default_box,
                )
            return dict_

        return _impl

    return _boxify


def chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def is_overlap(r1: str, r2: str) -> bool:
    """Check if two CIDR ranges overlap"""
    net1 = ipaddress.ip_network(r1)
    net2 = ipaddress.ip_network(r2)
    return net1.overlaps(net2) or net2.overlaps(net1)


def create_mc_client(ctx: Dict) -> Client:
    config: Config = ctx["config"]
    mc_client = Client(
        session=Session(
            endpoint=config.mcd_api_endpoint,
            mcd_id=config.mcd_id,
            mcd_token=config.mcd_token,
        )
    )
    return mc_client


def read_files(files: List) -> Iterator[str]:
    """Read a list of files"""
    for fp in files:
        yield fp.read()


def render_dumped_json(path: str, **kwargs) -> str:
    """Render and dump as formatted JSON"""
    return json.dumps(json.loads(render(path, **kwargs)), indent=4, sort_keys=True)


def render(path: str, **kwargs) -> str:
    """Load file from path and inject kwargs."""
    template = Template(pkgutil.get_data(__name__, path).decode("utf-8"))  # type: ignore
    return template.render(**kwargs)


def generate_token(length: Optional[int] = 16) -> str:
    """Generate a random url safe token"""
    return secrets.token_urlsafe(length)
