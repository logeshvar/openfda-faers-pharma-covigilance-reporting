from __future__ import annotations

from src.curated.build_patient_demo import (
    build_patient_demo_records,
    derive_age_band,
    derive_age_years,
    normalize_patient_demo_envelope,
)


def test_derive_age_years_converts_openfda_age_units() -> None:
    assert derive_age_years(7, "800") == 70
    assert derive_age_years(36, "802") == 3
    assert round(derive_age_years(14, "803") or 0, 2) == 0.27
    assert derive_age_years(None, "801") is None
    assert derive_age_years(10, None) is None


def test_derive_age_band_maps_expected_ranges() -> None:
    assert derive_age_band(None) == "Unknown"
    assert derive_age_band(17.99) == "0-17"
    assert derive_age_band(18) == "18-44"
    assert derive_age_band(45) == "45-64"
    assert derive_age_band(65) == "65+"


def test_normalize_patient_demo_envelope_maps_core_fields() -> None:
    raw_envelope = {
        "safetyreportid": "1001",
        "safetyreportversion": "2",
        "raw_payload": {
            "patient": {
                "patientsex": "2",
                "patientonsetage": "36",
                "patientonsetageunit": "801",
            }
        },
    }

    actual = normalize_patient_demo_envelope(raw_envelope)

    assert actual == {
        "safetyreportid": "1001",
        "safetyreportversion": "2",
        "patientsex_code": "2",
        "patientsex_label": "Female",
        "patientonsetage": 36.0,
        "patientonsetageunit_code": "801",
        "patientonsetageunit_label": "Year",
        "derived_age_years": 36.0,
        "derived_age_band": "18-44",
    }


def test_build_patient_demo_records_keeps_latest_record_per_report_version() -> None:
    older_envelope = {
        "safetyreportid": "2001",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_001",
        "load_timestamp": "2026-04-21T03:00:00Z",
        "raw_payload": {"patient": {"patientsex": "1"}},
    }
    newer_envelope = {
        "safetyreportid": "2001",
        "safetyreportversion": "1",
        "ingest_batch_id": "batch_002",
        "load_timestamp": "2026-04-21T04:00:00Z",
        "raw_payload": {"patient": {"patientsex": "2"}},
    }

    actual = build_patient_demo_records([older_envelope, newer_envelope])

    assert len(actual) == 1
    assert actual[0]["patientsex_code"] == "2"
    assert actual[0]["patientsex_label"] == "Female"
