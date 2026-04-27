from __future__ import annotations

from datetime import date, datetime
from functools import reduce
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def report_month_partitions(window_start: str | date, window_end: str | date) -> list[tuple[int, int]]:
    start = _parse_date(window_start)
    end = _parse_date(window_end)
    if end < start:
        raise ValueError("window_end must be on or after window_start")

    partitions: list[tuple[int, int]] = []
    year = start.year
    month = start.month
    while (year, month) <= (end.year, end.month):
        partitions.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return partitions


def report_month_replace_where(
    window_start: str | date,
    window_end: str | date,
    year_column: str = "report_year",
    month_column: str = "report_month",
) -> str:
    clauses = [
        f"({year_column} = {year} AND {month_column} = {month})"
        for year, month in report_month_partitions(window_start, window_end)
    ]
    return " OR ".join(clauses)


def filter_to_report_months(
    df: "DataFrame",
    window_start: str | date,
    window_end: str | date,
    year_column: str = "report_year",
    month_column: str = "report_month",
) -> "DataFrame":
    from pyspark.sql import functions as F

    conditions: list[Column] = [
        (F.col(year_column) == F.lit(year)) & (F.col(month_column) == F.lit(month))
        for year, month in report_month_partitions(window_start, window_end)
    ]
    return df.filter(reduce(lambda left, right: left | right, conditions))


def overwrite_report_month_partitions(
    df: "DataFrame",
    output_path: str,
    window_start: str | date,
    window_end: str | date,
    partition_columns: tuple[str, ...] = ("report_year", "report_month"),
) -> None:
    filtered_df = filter_to_report_months(df, window_start=window_start, window_end=window_end)
    replace_where = report_month_replace_where(window_start=window_start, window_end=window_end)
    (
        filtered_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy(*partition_columns)
        .save(output_path)
    )
