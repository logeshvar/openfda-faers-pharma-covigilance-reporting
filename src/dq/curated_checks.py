from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class DQCheckResult:
    check_name: str
    status: str
    failed_row_count: int
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DQSummary:
    overall_status: str
    total_records: int
    check_results: tuple[DQCheckResult, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "total_records": self.total_records,
            "check_results": [result.to_dict() for result in self.check_results],
        }


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == []


def _check_required_case_header_keys(records: list[dict[str, Any]]) -> DQCheckResult:
    failed_rows = 0
    for record in records:
        if _is_missing(record.get("safetyreportid")) or _is_missing(record.get("safetyreportversion")):
            failed_rows += 1

    status = "PASS" if failed_rows == 0 else "FAIL"
    return DQCheckResult(
        check_name="case_header_required_keys_present",
        status=status,
        failed_row_count=failed_rows,
        message=f"{failed_rows} curated_case_header row(s) are missing safetyreportid or safetyreportversion.",
    )


def _check_unique_case_header_keys(records: list[dict[str, Any]]) -> DQCheckResult:
    seen_keys: set[tuple[str | None, str | None]] = set()
    duplicate_rows = 0

    for record in records:
        key = (record.get("safetyreportid"), record.get("safetyreportversion"))
        if key in seen_keys:
            duplicate_rows += 1
            continue
        seen_keys.add(key)

    status = "PASS" if duplicate_rows == 0 else "FAIL"
    return DQCheckResult(
        check_name="case_header_unique_keys",
        status=status,
        failed_row_count=duplicate_rows,
        message=f"{duplicate_rows} duplicate curated_case_header key(s) were found.",
    )


def _check_report_date_partitions(records: list[dict[str, Any]]) -> DQCheckResult:
    failed_rows = 0
    for record in records:
        report_date_value = record.get("receipt_date") or record.get("receive_date")
        report_year = record.get("report_year")
        report_month = record.get("report_month")
        report_quarter = record.get("report_quarter")

        if _is_missing(report_date_value):
            if any(not _is_missing(value) for value in (report_year, report_month, report_quarter)):
                failed_rows += 1
            continue

        try:
            report_date = datetime.strptime(str(report_date_value), "%Y-%m-%d").date()
        except ValueError:
            failed_rows += 1
            continue

        expected_quarter = ((report_date.month - 1) // 3) + 1
        if (
            report_year != report_date.year
            or report_month != report_date.month
            or report_quarter != expected_quarter
        ):
            failed_rows += 1

    status = "PASS" if failed_rows == 0 else "FAIL"
    return DQCheckResult(
        check_name="case_header_report_partitions_align_to_dates",
        status=status,
        failed_row_count=failed_rows,
        message=f"{failed_rows} curated_case_header row(s) have partition columns that do not match the report date.",
    )


def _check_serious_case_indicator(records: list[dict[str, Any]]) -> DQCheckResult:
    failed_rows = 0
    flag_columns = (
        "serious_flag",
        "seriousness_death_flag",
        "seriousness_hospitalization_flag",
        "seriousness_lifethreatening_flag",
        "seriousness_disabling_flag",
        "seriousness_congenital_anomaly_flag",
        "seriousness_other_flag",
    )

    for record in records:
        expected_indicator = max(int(record.get(flag_name, 0) or 0) for flag_name in flag_columns)
        if int(record.get("serious_case_ind", 0) or 0) != expected_indicator:
            failed_rows += 1

    status = "PASS" if failed_rows == 0 else "FAIL"
    return DQCheckResult(
        check_name="case_header_serious_case_indicator_consistent",
        status=status,
        failed_row_count=failed_rows,
        message=f"{failed_rows} curated_case_header row(s) have a serious_case_ind value inconsistent with the seriousness flags.",
    )


def run_case_header_checks(records: list[dict[str, Any]]) -> DQSummary:
    checks = (
        _check_required_case_header_keys(records),
        _check_unique_case_header_keys(records),
        _check_report_date_partitions(records),
        _check_serious_case_indicator(records),
    )
    overall_status = "PASS" if all(check.status == "PASS" for check in checks) else "FAIL"
    return DQSummary(
        overall_status=overall_status,
        total_records=len(records),
        check_results=checks,
    )


def raise_for_failed_case_header_checks(summary: DQSummary) -> None:
    if summary.overall_status == "PASS":
        return

    failed_checks = [check for check in summary.check_results if check.status == "FAIL"]
    failure_messages = "; ".join(
        f"{check.check_name}: {check.message}" for check in failed_checks
    )
    raise ValueError(f"curated_case_header DQ checks failed. {failure_messages}")
