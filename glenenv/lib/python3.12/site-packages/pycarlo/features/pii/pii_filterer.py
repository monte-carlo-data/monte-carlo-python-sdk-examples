import logging
import re
import time
from dataclasses import dataclass
from re import Pattern
from typing import Any, Callable, Dict, List, Optional, Union

from dataclasses_json import dataclass_json

_logger = logging.getLogger(__name__)


@dataclass_json
@dataclass
class PiiActiveFilter:
    name: str
    pattern: str


@dataclass_json
@dataclass
class PiiActiveFiltersConfig:
    active: List[PiiActiveFilter]
    fail_closed: bool = True

    @staticmethod
    def is_valid_config(cfg: Optional[Dict]) -> bool:
        return bool(cfg and cfg.get("active", []))


@dataclass_json
@dataclass
class PiiFilterMetrics:
    replacements: int
    time_taken_ms: float


@dataclass
class PiiCompiledFilter:
    name: str
    compiled_expression: Pattern
    replacement: str


class PiiFilterer:
    _TOTAL_KEY = "_total"
    _REPLACEMENT_STRING = "<filtered:{}>"

    def __init__(self, filters_config: Optional[Dict], include_metrics: bool = True):
        self._config = (
            PiiActiveFiltersConfig.from_dict(filters_config)  # type: ignore
            if PiiActiveFiltersConfig.is_valid_config(filters_config)
            else None
        )

        if self._config:
            self._filters = [
                PiiCompiledFilter(
                    name=f.name,
                    compiled_expression=re.compile(f.pattern),
                    replacement=self._REPLACEMENT_STRING.format(f.name),
                )
                for f in self._config.active
            ]
        self._include_metrics = include_metrics

    @staticmethod
    def _metrics_to_final(metrics: Dict, elapsed_time_ns: int) -> Dict:
        final_metrics = {
            k: PiiFilterMetrics(replacements=v, time_taken_ms=t / 1000000).to_dict()  # type: ignore
            for k, (v, t) in metrics.items()
        }
        total_replacements = sum(m["replacements"] for m in final_metrics.values())

        final_metrics[PiiFilterer._TOTAL_KEY] = PiiFilterMetrics(
            replacements=total_replacements, time_taken_ms=elapsed_time_ns / 1000000
        ).to_dict()  # type: ignore

        return final_metrics

    def filter_content(self, content: Union[bytes, str, Dict]) -> Union[bytes, str, Dict]:
        """
        Utility method to filter one of bytes, str or Dict. Calls internally one of:
        - filter_data
        - filter_str
        - filter_message
        """
        if isinstance(content, dict):
            return self.filter_message(content)
        elif isinstance(content, str):
            return self.filter_str(content)
        else:
            return self.filter_data(content)

    def filter_message(self, msg: Union[bytes, str, Dict]) -> Union[bytes, str, Dict]:
        """
        Filters a dictionary or a list. If the object is a dictionary it includes metrics in a
        pii_metrics attribute in the result dictionary (only if the filterer was created with
        include_metrics=True which is the default value).
        """
        if not self._config:
            return msg

        start_time = time.time_ns()

        include_metrics = self._include_metrics and isinstance(msg, Dict)
        metrics = {} if include_metrics else None

        result = self._do_filter(msg, lambda o: self._filter_object(o, metrics=metrics))

        if include_metrics:
            assert metrics is not None
            elapsed_time = time.time_ns() - start_time
            result["pii_metrics"] = self._metrics_to_final(metrics, elapsed_time_ns=elapsed_time)

        return result

    def _do_filter(self, data: Any, filter_function: Callable[[Any], Any]) -> Any:
        if not self._config:
            return data
        try:
            result = filter_function(data)
        except Exception as exc:
            if self._config.fail_closed:
                raise
            _logger.exception(
                f"Failed to evaluate PII filters: {exc}, ignoring because fail_closed=False"
            )
            result = data
        return result

    def filter_str(self, msg: str) -> str:
        """
        Filters a string, please note metrics are not included as we don't have a way to include
        metrics in the resulting string. We might return metrics in the future if we decide to
        use them where we call this (dbtv2 collection)
        """
        return self._do_filter(msg, lambda o: self._filter_text(o))

    def filter_data(self, msg: bytes, encoding: str = "utf8") -> bytes:
        """
        Filters a bytes array using the given encoding, please note metrics are not included as
        we don't have a way to include metrics in the resulting bytes array. We might return metrics
        in the future if we decide to use them where we call this (dbtv2 collection)
        """
        return self._do_filter(msg, lambda o: self._filter_bytes(o, encoding=encoding))

    def _filter_bytes(self, data: bytes, encoding: str) -> bytes:
        text = data.decode(encoding)
        filtered_text = self._filter_text(text)
        return filtered_text.encode(encoding)

    def _filter_object(self, o: Any, metrics: Optional[Dict] = None) -> Any:
        if isinstance(o, Dict):
            return {
                self._filter_text(k, metrics=metrics): self._filter_object(v, metrics=metrics)
                for k, v in o.items()
            }
        elif isinstance(o, List):
            return [self._filter_object(e, metrics=metrics) for e in o]
        elif isinstance(o, tuple):
            return tuple(self._filter_object(e, metrics=metrics) for e in o)
        elif isinstance(o, str):
            return self._filter_text(o, metrics=metrics)
        else:
            return o

    def _filter_text(self, text: str, metrics: Optional[Dict] = None) -> str:
        for f in self._filters:
            filter_name = f.name
            start_time = time.time_ns()
            text, updated_count = f.compiled_expression.subn(f.replacement, text)
            elapsed_time = time.time_ns() - start_time

            if metrics is not None:
                prev_count, prev_time = metrics.get(filter_name, (0, 0))
                metrics[filter_name] = (prev_count + updated_count, prev_time + elapsed_time)

        return text
