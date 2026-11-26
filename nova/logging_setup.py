"""Structured JSON logging for the Nova fleet tracking system.

All Nova services emit single-line JSON records so that logs can be parsed
and indexed by the platform's log pipeline. The log level is resolved from
the explicit argument, then ``NOVA_LOG_LEVEL``, then ``INFO``.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

ENV_LOG_LEVEL = "NOVA_LOG_LEVEL"
DEFAULT_LOG_LEVEL = "INFO"

# Attribute used to tag handlers installed by this module.
_NOVA_HANDLER_ATTR = "_nova_json_handler"


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def _resolve_level(level: Optional[Union[str, int]]) -> int:
    candidate: Union[str, int] = level or os.environ.get(ENV_LOG_LEVEL) or DEFAULT_LOG_LEVEL
    if isinstance(candidate, int):
        return candidate

    resolved = logging.getLevelName(str(candidate).strip().upper())
    if not isinstance(resolved, int):
        raise ValueError(f"Unknown log level: {candidate!r}")
    return resolved


def setup_logging(level: Optional[Union[str, int]] = None) -> logging.Logger:
    """Configure root logging with a JSON formatter.

    Args:
        level: Explicit log level (name or numeric). When omitted,
            ``NOVA_LOG_LEVEL`` is used, falling back to ``INFO``.

    Returns:
        The configured root logger.

    Raises:
        ValueError: If the resolved level name is not recognised.
    """
    resolved = _resolve_level(level)

    root = logging.getLogger()
    root.setLevel(resolved)

    # Remove handlers previously installed by this module to keep setup idempotent.
    for handler in list(root.handlers):
        if getattr(handler, _NOVA_HANDLER_ATTR, False):
            root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.setLevel(resolved)
    setattr(handler, _NOVA_HANDLER_ATTR, True)
    root.addHandler(handler)

    return root


def get_logger(name: str) -> logging.Logger:
    """Return a named logger for use across Nova modules."""
    return logging.getLogger(name)


def log_extra(**fields: Any) -> Dict[str, Any]:
    """Build the ``extra`` mapping for structured log fields.

    Usage::

        logger.info("telemetry received", extra=log_extra(vehicle_id="v-1"))
    """
    return {"extra_fields": fields}
