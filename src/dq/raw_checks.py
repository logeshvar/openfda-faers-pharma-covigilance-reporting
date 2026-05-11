from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable


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


def _check_minimum_records(records: list[dict[str, Any]], min_expected_records: int) -> DQCheckResult:
    total_records = len(records)
    status = "PASS" if total_records >= min_expected_records else "FAIL"
    return DQCheckResult(
        check_name="minimum_record_volume",
        status=status,
        failed_row_count=0 if status == "PASS" else 1,
        message=f"Expected at least {min_expected_records} record(s); found {total_records}.",
    )


def _check_required_fields(
    records: list[dict[str, Any]],
    required_fields: tuple[str, ...],
) -> DQCheckResult:
    failed_rows = 0
    for record in records:
        if any(_is_missing(record.get(field_name)) for field_name in required_fields):
            failed_rows += 1

    status = "PASS" if failed_rows == 0 else "FAIL"
    return DQCheckResult(
        check_name="required_raw_fields_present",
        status=status,
        failed_row_count=failed_rows,
        message=f"{failed_rows} record(s) are missing one or more required fields: {list(required_fields)}.",
    )


def _check_unique_report_versions(records: list[dict[str, Any]]) -> DQCheckResult:
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
        check_name="unique_safetyreportid_version_within_batch",
        status=status,
        failed_row_count=duplicate_rows,
        message=f"{duplicate_rows} duplicate report version row(s) found within the extracted batch.",
    )


def _check_receivedate_format(records: list[dict[str, Any]]) -> DQCheckResult:
    failed_rows = 0
    for record in records:
        receivedate = record.get("receivedate")
        if _is_missing(receivedate):
            failed_rows += 1
            continue
        try:
            datetime.strptime(str(receivedate), "%Y%m%d")
        except ValueError:
            failed_rows += 1

    status = "PASS" if failed_rows == 0 else "FAIL"
    return DQCheckResult(
        check_name="receivedate_yyyymmdd_format",
        status=status,
        failed_row_count=failed_rows,
        message=f"{failed_rows} record(s) have a missing or invalid receivedate value.",
    )


def run_raw_checks(
    records: list[dict[str, Any]],
    required_fields: tuple[str, ...],
    min_expected_records: int = 0,
) -> DQSummary:
    checks = (
        _check_minimum_records(records, min_expected_records=min_expected_records),
        _check_required_fields(records, required_fields=required_fields),
        _check_unique_report_versions(records),
        _check_receivedate_format(records),
    )
    overall_status = "PASS" if all(check.status == "PASS" for check in checks) else "FAIL"
    return DQSummary(
        overall_status=overall_status,
        total_records=len(records),
        check_results=checks,
    )


def run_raw_checks_from_iterable(
    records: Iterable[dict[str, Any]],
    required_fields: tuple[str, ...],
    min_expected_records: int = 0,
) -> DQSummary:
    """Run raw checks in one pass without materializing the whole batch."""

    total_records = 0
    required_field_failures = 0
    duplicate_rows = 0
    receivedate_failures = 0
    seen_keys: set[tuple[str | None, str | None]] = set()

    for record in records:
        total_records += 1

        if any(_is_missing(record.get(field_name)) for field_name in required_fields):
            required_field_failures += 1

        key = (record.get("safetyreportid"), record.get("safetyreportversion"))
        if key in seen_keys:
            duplicate_rows += 1
        else:
            seen_keys.add(key)

        receivedate = record.get("receivedate")
        if _is_missing(receivedate):
            receivedate_failures += 1
        else:
            try:
                datetime.strptime(str(receivedate), "%Y%m%d")
            except ValueError:
                receivedate_failures += 1

    volume_status = "PASS" if total_records >= min_expected_records else "FAIL"
    required_status = "PASS" if required_field_failures == 0 else "FAIL"
    duplicate_status = "PASS" if duplicate_rows == 0 else "FAIL"
    receivedate_status = "PASS" if receivedate_failures == 0 else "FAIL"
    checks = (
        DQCheckResult(
            check_name="minimum_record_volume",
            status=volume_status,
            failed_row_count=0 if volume_status == "PASS" else 1,
            message=f"Expected at least {min_expected_records} record(s); found {total_records}.",
        ),
        DQCheckResult(
            check_name="required_raw_fields_present",
            status=required_status,
            failed_row_count=required_field_failures,
            message=(
                f"{required_field_failures} record(s) are missing one or more required fields: "
                f"{list(required_fields)}."
            ),
        ),
        DQCheckResult(
            check_name="unique_safetyreportid_version_within_batch",
            status=duplicate_status,
            failed_row_count=duplicate_rows,
            message=f"{duplicate_rows} duplicate report version row(s) found within the extracted batch.",
        ),
        DQCheckResult(
            check_name="receivedate_yyyymmdd_format",
            status=receivedate_status,
            failed_row_count=receivedate_failures,
            message=f"{receivedate_failures} record(s) have a missing or invalid receivedate value.",
        ),
    )
    overall_status = "PASS" if all(check.status == "PASS" for check in checks) else "FAIL"
    return DQSummary(overall_status=overall_status, total_records=total_records, check_results=checks)


def raise_for_failed_checks(summary: DQSummary) -> None:
    if summary.overall_status == "PASS":
        return

    failed_checks = [check for check in summary.check_results if check.status == "FAIL"]
    failure_messages = "; ".join(
        f"{check.check_name}: {check.message}" for check in failed_checks
    )
    raise ValueError(f"Raw DQ checks failed. {failure_messages}")
