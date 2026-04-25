from __future__ import annotations

from src.dq.curated_checks import run_case_header_checks


def _valid_case_header_record() -> dict[str, object]:
    return {
        "safetyreportid": "1001",
        "safetyreportversion": "1",
        "receipt_date": "2024-04-15",
        "receive_date": "2024-04-16",
        "report_year": 2024,
        "report_month": 4,
        "report_quarter": 2,
        "serious_flag": 0,
        "seriousness_death_flag": 0,
        "seriousness_hospitalization_flag": 1,
        "seriousness_lifethreatening_flag": 0,
        "seriousness_disabling_flag": 0,
        "seriousness_congenital_anomaly_flag": 0,
        "seriousness_other_flag": 0,
        "serious_case_ind": 1,
    }


def test_run_case_header_checks_passes_for_valid_records() -> None:
    summary = run_case_header_checks([_valid_case_header_record()])

    assert summary.overall_status == "PASS"
    assert all(result.status == "PASS" for result in summary.check_results)


def test_run_case_header_checks_fails_for_duplicate_keys() -> None:
    record = _valid_case_header_record()

    summary = run_case_header_checks([record, dict(record)])

    assert summary.overall_status == "FAIL"
    duplicate_check = next(
        result for result in summary.check_results if result.check_name == "case_header_unique_keys"
    )
    assert duplicate_check.status == "FAIL"
    assert duplicate_check.failed_row_count == 1


def test_run_case_header_checks_fails_for_partition_mismatch() -> None:
    record = _valid_case_header_record()
    record["report_month"] = 3

    summary = run_case_header_checks([record])

    assert summary.overall_status == "FAIL"
    partition_check = next(
        result
        for result in summary.check_results
        if result.check_name == "case_header_report_partitions_align_to_dates"
    )
    assert partition_check.status == "FAIL"


def test_run_case_header_checks_fails_for_serious_case_indicator_mismatch() -> None:
    record = _valid_case_header_record()
    record["serious_case_ind"] = 0

    summary = run_case_header_checks([record])

    assert summary.overall_status == "FAIL"
    indicator_check = next(
        result
        for result in summary.check_results
        if result.check_name == "case_header_serious_case_indicator_consistent"
    )
    assert indicator_check.status == "FAIL"
