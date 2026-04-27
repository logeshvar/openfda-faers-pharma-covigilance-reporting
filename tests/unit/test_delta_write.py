from __future__ import annotations

from datetime import date

import pytest

from src.common.delta_write import report_month_partitions, report_month_replace_where


def test_report_month_partitions_returns_inclusive_months() -> None:
    assert report_month_partitions("2025-12-28", "2026-02-02") == [
        (2025, 12),
        (2026, 1),
        (2026, 2),
    ]


def test_report_month_partitions_accepts_date_values() -> None:
    assert report_month_partitions(date(2026, 1, 1), date(2026, 1, 31)) == [(2026, 1)]


def test_report_month_partitions_rejects_inverted_window() -> None:
    with pytest.raises(ValueError, match="window_end must be on or after window_start"):
        report_month_partitions("2026-02-01", "2026-01-31")


def test_report_month_replace_where_builds_delta_predicate() -> None:
    assert (
        report_month_replace_where("2025-12-28", "2026-01-02")
        == "(report_year = 2025 AND report_month = 12) OR "
        "(report_year = 2026 AND report_month = 1)"
    )
