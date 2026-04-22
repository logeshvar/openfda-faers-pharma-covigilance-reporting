from __future__ import annotations

from src.curated.build_case_header import (
    build_case_header_records,
    normalize_case_header_envelope,
)


def test_normalize_case_header_envelope_maps_core_fields() -> None:
    raw_envelope = {
        "safetyreportid": "1001",
        "safetyreportversion": "3",
        "ingest_batch_id": "batch_001",
        "source_file_name": "openfda_batch.ndjson",
        "load_timestamp": "2026-04-21T03:00:00Z",
        "raw_payload": {
            "companynumb": "CN-123",
            "duplicate": "1",
            "occurcountry": "DE",
            "receiptdate": "20240415",
            "receivedate": "20240416",
            "reporttype": "1",
            "serious": "2",
            "seriousnessdeath": "2",
            "seriousnesshospitalization": "1",
            "seriousnesslifethreatening": "2",
            "seriousnessdisabling": "2",
            "seriousnesscongenitalanomali": "2",
            "seriousnessother": "2",
        },
    }

    actual = normalize_case_header_envelope(raw_envelope)

    assert actual == {
        "safetyreportid": "1001",
        "safetyreportversion": "3",
        "companynumb": "CN-123",
        "duplicate_flag": 1,
        "occur_country": "DE",
        "receipt_date": "2024-04-15",
        "receive_date": "2024-04-16",
        "report_type_code": "1",
        "serious_flag": 0,
        "seriousness_death_flag": 0,
        "seriousness_hospitalization_flag": 1,
        "seriousness_lifethreatening_flag": 0,
        "seriousness_disabling_flag": 0,
        "seriousness_congenital_anomaly_flag": 0,
        "seriousness_other_flag": 0,
        "ingest_batch_id": "batch_001",
        "source_file_name": "openfda_batch.ndjson",
        "load_timestamp": "2026-04-21T03:00:00Z",
        "report_year": 2024,
        "report_month": 4,
        "report_quarter": 2,
        "serious_case_ind": 1,
    }


def test_normalize_case_header_envelope_uses_country_and_duplicate_fallbacks() -> None:
    raw_envelope = {
        "safetyreportid": "1002",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_002",
        "source_file_name": "openfda_batch.ndjson",
        "load_timestamp": "2026-04-21T03:00:00Z",
        "raw_payload": {
            "primarysourcecountry": "FR",
            "receivedate": "20240330",
            "reporttype": "2",
            "serious": "1",
            "seriousnessdeath": "2",
            "seriousnesshospitalization": "2",
            "seriousnesslifethreatening": "2",
            "seriousnessdisabling": "2",
            "seriousnesscongenitalanomali": "2",
            "seriousnessother": "2",
            "reportduplicate": {"duplicatenumb": "FR-ABC-1"},
            "primarysource": {"reportercountry": "GB"},
        },
    }

    actual = normalize_case_header_envelope(raw_envelope)

    assert actual["duplicate_flag"] == 1
    assert actual["occur_country"] == "FR"
    assert actual["receipt_date"] is None
    assert actual["receive_date"] == "2024-03-30"
    assert actual["report_year"] == 2024
    assert actual["report_month"] == 3
    assert actual["report_quarter"] == 1
    assert actual["serious_flag"] == 1
    assert actual["serious_case_ind"] == 1


def test_build_case_header_records_keeps_latest_record_per_report_version() -> None:
    older_envelope = {
        "safetyreportid": "2001",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_001",
        "source_file_name": "older.ndjson",
        "load_timestamp": "2026-04-21T03:00:00Z",
        "raw_payload": {
            "companynumb": "OLD",
            "receivedate": "20240401",
            "receiptdate": "20240401",
            "reporttype": "1",
            "serious": "2",
            "seriousnessdeath": "2",
            "seriousnesshospitalization": "2",
            "seriousnesslifethreatening": "2",
            "seriousnessdisabling": "2",
            "seriousnesscongenitalanomali": "2",
            "seriousnessother": "2",
        },
    }
    newer_envelope = {
        "safetyreportid": "2001",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_002",
        "source_file_name": "newer.ndjson",
        "load_timestamp": "2026-04-21T04:00:00Z",
        "raw_payload": {
            "companynumb": "NEW",
            "receivedate": "20240401",
            "receiptdate": "20240401",
            "reporttype": "1",
            "serious": "1",
            "seriousnessdeath": "2",
            "seriousnesshospitalization": "2",
            "seriousnesslifethreatening": "2",
            "seriousnessdisabling": "2",
            "seriousnesscongenitalanomali": "2",
            "seriousnessother": "2",
        },
    }

    actual = build_case_header_records([older_envelope, newer_envelope])

    assert len(actual) == 1
    assert actual[0]["companynumb"] == "NEW"
    assert actual[0]["source_file_name"] == "newer.ndjson"
    assert actual[0]["serious_flag"] == 1
