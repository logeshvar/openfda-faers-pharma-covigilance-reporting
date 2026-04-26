from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def as_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        logger.warning("Unable to parse numeric value=%s", value)
        return None


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]
