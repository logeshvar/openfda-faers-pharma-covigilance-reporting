from __future__ import annotations

from datetime import date

import pytest

from src.ingestion.windowing import resolve_lagged_daily_window


def test_resolve_lagged_daily_window_uses_stable_source_lag() -> None:
    window_start, window_end = resolve_lagged_daily_window(
        reference_date=date(2026, 4, 26),
        source_lag_days=120,
        default_window_days=1,
    )

    assert window_start == date(2025, 12, 27)
    assert window_end == date(2025, 12, 27)


def test_resolve_lagged_daily_window_can_expand_default_window() -> None:
    window_start, window_end = resolve_lagged_daily_window(
        reference_date=date(2026, 4, 26),
        source_lag_days=120,
        default_window_days=7,
    )

    assert window_start == date(2025, 12, 21)
    assert window_end == date(2025, 12, 27)


def test_resolve_lagged_daily_window_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="source_lag_days"):
        resolve_lagged_daily_window(date(2026, 4, 26), source_lag_days=-1, default_window_days=1)

    with pytest.raises(ValueError, match="default_window_days"):
        resolve_lagged_daily_window(date(2026, 4, 26), source_lag_days=120, default_window_days=0)
