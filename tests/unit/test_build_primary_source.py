from __future__ import annotations

from src.curated.build_primary_source import (
    build_primary_source_records,
    normalize_primary_source_envelope,
)


def test_normalize_primary_source_envelope_maps_core_fields() -> None:
    raw_envelope = {
        "safetyreportid": "1001",
        "safetyreportversion": "2",
        "raw_payload": {
            "primarysource": {
                "qualification": "1",
                "reportercountry": "US",
            }
        },
    }

    actual = normalize_primary_source_envelope(raw_envelope)

    assert actual == {
        "safetyreportid": "1001",
        "safetyreportversion": "2",
        "qualification_code": "1",
        "qualification_label": "Physician",
        "reporter_country": "US",
    }


def test_build_primary_source_records_keeps_latest_record_per_report_version() -> None:
    older_envelope = {
        "safetyreportid": "2001",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_001",
        "load_timestamp": "2026-04-21T03:00:00Z",
        "raw_payload": {"primarysource": {"qualification": "2", "reportercountry": "FR"}},
    }
    newer_envelope = {
        "safetyreportid": "2001",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_002",
        "load_timestamp": "2026-04-21T04:00:00Z",
        "raw_payload": {"primarysource": {"qualification": "5", "reportercountry": "GB"}},
    }

    actual = build_primary_source_records([older_envelope, newer_envelope])

    assert len(actual) == 1
    assert actual[0]["qualification_code"] == "5"
    assert actual[0]["qualification_label"] == "Consumer or Non-Health Professional"
    assert actual[0]["reporter_country"] == "GB"
