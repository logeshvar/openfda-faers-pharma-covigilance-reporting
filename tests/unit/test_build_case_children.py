from __future__ import annotations

from src.curated.build_case_drug import build_case_drug_records
from src.curated.build_case_drug_openfda import build_case_drug_openfda_records
from src.curated.build_case_reaction import build_case_reaction_records


def _raw_envelope() -> dict[str, object]:
    return {
        "safetyreportid": "1001",
        "safetyreportversion": "2",
        "ingest_batch_id": "batch_001",
        "load_timestamp": "2026-04-25T10:00:00Z",
        "raw_payload": {
            "patient": {
                "drug": [
                    {
                        "medicinalproduct": "Drug A",
                        "activesubstance": {"activesubstancename": "Substance A"},
                        "drugcharacterization": "1",
                        "drugindication": "Pain",
                        "actiondrug": "1",
                        "drugadministrationroute": "048",
                        "openfda": {
                            "generic_name": ["GEN A"],
                            "brand_name": ["BRAND A"],
                            "manufacturer_name": ["ACME"],
                            "pharm_class_epc": ["Analgesic"],
                            "substance_name": ["Substance A"],
                        },
                    }
                ],
                "reaction": [
                    {
                        "reactionmeddrapt": "Headache",
                        "reactionoutcome": "2",
                    }
                ],
            }
        },
    }


def test_build_case_drug_records_maps_child_drugs() -> None:
    actual = build_case_drug_records([_raw_envelope()])

    assert actual == [
        {
            "safetyreportid": "1001",
            "safetyreportversion": "2",
            "drug_seq_num": 1,
            "medicinal_product": "Drug A",
            "active_substance_name": "Substance A",
            "drug_characterization_code": "1",
            "drug_characterization_label": "Suspect",
            "drug_indication": "Pain",
            "actiondrug_code": "1",
            "actiondrug_label": "Drug Withdrawn",
            "drug_administration_route_code": "048",
        }
    ]


def test_build_case_drug_openfda_records_maps_arrays() -> None:
    actual = build_case_drug_openfda_records([_raw_envelope()])

    assert actual[0]["generic_name_arr"] == ["GEN A"]
    assert actual[0]["brand_name_arr"] == ["BRAND A"]
    assert actual[0]["manufacturer_name_arr"] == ["ACME"]
    assert actual[0]["pharm_class_epc_arr"] == ["Analgesic"]
    assert actual[0]["substance_name_arr"] == ["Substance A"]


def test_build_case_reaction_records_maps_child_reactions() -> None:
    actual = build_case_reaction_records([_raw_envelope()])

    assert actual == [
        {
            "safetyreportid": "1001",
            "safetyreportversion": "2",
            "reaction_seq_num": 1,
            "reaction_meddra_pt": "Headache",
            "reaction_outcome_code": "2",
            "reaction_outcome_label": "Recovering/Resolving",
        }
    ]
