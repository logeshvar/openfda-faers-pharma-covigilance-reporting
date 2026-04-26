from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


def _coerce_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    raise TypeError(f"Cannot resolve a date from value={value!r}")


def resolve_lagged_daily_window(
    reference_date: Any,
    source_lag_days: int,
    default_window_days: int,
) -> tuple[date, date]:
    if source_lag_days < 0:
        raise ValueError("source_lag_days must be greater than or equal to 0")
    if default_window_days < 1:
        raise ValueError("default_window_days must be greater than or equal to 1")

    window_end = _coerce_date(reference_date) - timedelta(days=source_lag_days)
    window_start = window_end - timedelta(days=default_window_days - 1)
    return window_start, window_end
