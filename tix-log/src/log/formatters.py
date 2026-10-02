"""
Rendering and reusable event processors.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .levels import TRACE


class JsonFormatter(
    logging.Formatter,
):
    """
    Render structured events and ordinary LogRecords as one JSON object per line.
    """

    def __init__(
        self: JsonFormatter,
        *,
        dumps: Callable[[Any], str] | None = None,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            dumps: Optional serializer returning a JSON string.
        """
        super().__init__()
        self._dumps: Callable[[Any], str] = dumps or (
            lambda value: json.dumps(value, default=str, ensure_ascii=False)
        )

    def format(
        self: JsonFormatter,
        record: logging.LogRecord,
    ) -> str:
        """
        Render the log record as a structured JSON string.

        Args:
            record: Log record to render or deliver.
        """
        event: dict[Any, Any] = dict(getattr(record, "event", {}))
        if "message" not in event:
            event["message"] = record.getMessage()
        event.setdefault("level", "trace" if record.levelno == TRACE else record.levelname.lower())
        event.setdefault("logger", record.name)
        event.setdefault("timestamp", record.created)
        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)
        return self._dumps(event)


def redact(
    keys: Iterable[str],
    replacement: str = "[REDACTED]",
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """
    Copy and redact matching mapping keys recursively (case insensitive).

    Input must be an acyclic tree. Lists and tuples are traversed; arbitrary objects
    are left intact, so applications should not log opaque secret-bearing objects.

    Args:
        keys: Field names to remove or redact.
        replacement: Value substituted for matching secret fields.
    """
    secrets: frozenset[str] = frozenset(key.casefold() for key in keys)

    def clean(
        value: Any,
    ) -> Any:
        """
        Recursively copy a value while replacing matching secret fields.

        Args:
            value: Value to inspect or recursively transform.
        """
        if isinstance(value, Mapping):
            return {
                key: replacement if str(key).casefold() in secrets else clean(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [clean(item) for item in value]
        return value

    def processor(
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Redact matching keys throughout a structured event.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        return {
            key: replacement if key.casefold() in secrets else clean(value)
            for key, value in event.items()
        }

    return processor
