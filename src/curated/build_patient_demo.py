from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping

import sys
from pathlib import Path

_repo_anchors = []
if "__file__" in globals():
    _repo_anchors.append(Path(__file__).resolve())
_repo_anchors.append(Path.cwd().resolve())

for _anchor in _repo_anchors:
    _start = _anchor if _anchor.is_dir() else _anchor.parent
    for _parent in (_start, *_start.parents):
        if (_parent / "src").is_dir() and (_parent / "conf").is_dir():
            if str(_parent) not in sys.path:
                sys.path.insert(0, str(_parent))
            break
    else:
        continue
    break

from src.common.databricks_runtime import add_common_databricks_args, log_common_databricks_args
from src.common.normalization import as_float as _as_float
from src.common.normalization import as_text as _as_text

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CURATED_PATIENT_DEMO_TABLE = "curated_patient_demo"
PATIENT_DEMO_KEY_COLUMNS = ("safetyreportid", "safetyreportversion")
PATIENT_DEMO_PARTITION_COLUMNS: tuple[str, ...] = ()

SEX_LABELS = {
    "0": "Unknown",
    "1": "Male",
    "2": "Female",
}

AGE_UNIT_LABELS = {
    "800": "Decade",
    "801": "Year",
    "802": "Month",
    "803": "Week",
    "804": "Day",
    "805": "Hour",
}


@dataclass(frozen=True)
class CuratedPatientDemoJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def patientsex_label(patientsex_code: Any) -> str | None:
    code = _as_text(patientsex_code)
    if code is None:
        return None
    return SEX_LABELS.get(code, "Unknown")


def patientonsetageunit_label(age_unit_code: Any) -> str | None:
    code = _as_text(age_unit_code)
    if code is None:
        return None
    return AGE_UNIT_LABELS.get(code, "Unknown")


def derive_age_years(patientonsetage: Any, patientonsetageunit_code: Any) -> float | None:
    age_value = _as_float(patientonsetage)
    unit_code = _as_text(patientonsetageunit_code)

    if age_value is None or unit_code is None:
        return None
    if unit_code == "800":
        return age_value * 10
    if unit_code == "801":
        return age_value
    if unit_code == "802":
        return age_value / 12
    if unit_code == "803":
        return age_value / 52.1775
    if unit_code == "804":
        return age_value / 365.25
    if unit_code == "805":
        return age_value / 8766
    return None


def derive_age_band(age_years: float | None) -> str:
    if age_years is None:
        return "Unknown"
    if age_years < 18:
        return "0-17"
    if age_years < 45:
        return "18-44"
    if age_years < 65:
        return "45-64"
    return "65+"


def normalize_patient_demo_envelope(raw_envelope: Mapping[str, Any]) -> dict[str, Any]:
    raw_payload = raw_envelope.get("raw_payload") or {}
    patient = raw_payload.get("patient") or {}
    patientsex_code = _as_text(patient.get("patientsex"))
    patientonsetageunit_code = _as_text(patient.get("patientonsetageunit"))
    patientonsetage = _as_float(patient.get("patientonsetage"))
    age_years = derive_age_years(patientonsetage, patientonsetageunit_code)

    return {
        "safetyreportid": _as_text(raw_envelope.get("safetyreportid") or raw_payload.get("safetyreportid")),
        "safetyreportversion": _as_text(
            raw_envelope.get("safetyreportversion") or raw_payload.get("safetyreportversion")
        ),
        "patientsex_code": patientsex_code,
        "patientsex_label": patientsex_label(patientsex_code),
        "patientonsetage": patientonsetage,
        "patientonsetageunit_code": patientonsetageunit_code,
        "patientonsetageunit_label": patientonsetageunit_label(patientonsetageunit_code),
        "derived_age_years": age_years,
        "derived_age_band": derive_age_band(age_years),
    }


def _sort_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("load_timestamp")) or "",
        _as_text(record.get("ingest_batch_id")) or "",
    )


def deduplicate_patient_demo_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str | None, str | None], dict[str, Any]] = {}

    for record in records:
        normalized_record = dict(record)
        key = (
            _as_text(normalized_record.get("safetyreportid")),
            _as_text(normalized_record.get("safetyreportversion")),
        )
        existing_record = latest_by_key.get(key)
        if existing_record is None or _sort_key(normalized_record) >= _sort_key(existing_record):
            latest_by_key[key] = normalized_record

    return list(latest_by_key.values())


def build_patient_demo_records(raw_envelopes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized_records = [normalize_patient_demo_envelope(raw_envelope) for raw_envelope in raw_envelopes]
    return deduplicate_patient_demo_records(normalized_records)


def _spark_age_years_expr(age_col: "Any", unit_col: "Any") -> "Any":
    from pyspark.sql import functions as F

    return (
        F.when(unit_col == F.lit("800"), age_col * F.lit(10.0))
        .when(unit_col == F.lit("801"), age_col)
        .when(unit_col == F.lit("802"), age_col / F.lit(12.0))
        .when(unit_col == F.lit("803"), age_col / F.lit(52.1775))
        .when(unit_col == F.lit("804"), age_col / F.lit(365.25))
        .when(unit_col == F.lit("805"), age_col / F.lit(8766.0))
        .otherwise(F.lit(None).cast("double"))
    )


def transform_patient_demo_df(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    patientsex_code_col = F.col("raw_payload.patient.patientsex").cast("string")
    age_col = F.col("raw_payload.patient.patientonsetage").cast("double")
    age_unit_col = F.col("raw_payload.patient.patientonsetageunit").cast("string")
    age_years_expr = _spark_age_years_expr(age_col=age_col, unit_col=age_unit_col)

    sex_label_expr = (
        F.when(patientsex_code_col == F.lit("0"), F.lit("Unknown"))
        .when(patientsex_code_col == F.lit("1"), F.lit("Male"))
        .when(patientsex_code_col == F.lit("2"), F.lit("Female"))
        .when(patientsex_code_col.isNull(), F.lit(None).cast("string"))
        .otherwise(F.lit("Unknown"))
    )
    age_unit_label_expr = (
        F.when(age_unit_col == F.lit("800"), F.lit("Decade"))
        .when(age_unit_col == F.lit("801"), F.lit("Year"))
        .when(age_unit_col == F.lit("802"), F.lit("Month"))
        .when(age_unit_col == F.lit("803"), F.lit("Week"))
        .when(age_unit_col == F.lit("804"), F.lit("Day"))
        .when(age_unit_col == F.lit("805"), F.lit("Hour"))
        .when(age_unit_col.isNull(), F.lit(None).cast("string"))
        .otherwise(F.lit("Unknown"))
    )
    age_band_expr = (
        F.when(age_years_expr.isNull(), F.lit("Unknown"))
        .when(age_years_expr < F.lit(18.0), F.lit("0-17"))
        .when(age_years_expr < F.lit(45.0), F.lit("18-44"))
        .when(age_years_expr < F.lit(65.0), F.lit("45-64"))
        .otherwise(F.lit("65+"))
    )

    transformed_df = raw_df.select(
        F.coalesce(F.col("safetyreportid"), F.col("raw_payload.safetyreportid")).alias("safetyreportid"),
        F.coalesce(F.col("safetyreportversion"), F.col("raw_payload.safetyreportversion")).alias(
            "safetyreportversion"
        ),
        patientsex_code_col.alias("patientsex_code"),
        sex_label_expr.alias("patientsex_label"),
        age_col.alias("patientonsetage"),
        age_unit_col.alias("patientonsetageunit_code"),
        age_unit_label_expr.alias("patientonsetageunit_label"),
        age_years_expr.alias("derived_age_years"),
        age_band_expr.alias("derived_age_band"),
        F.col("ingest_batch_id").cast("string").alias("_ingest_batch_id"),
        F.col("load_timestamp").cast("timestamp").alias("_load_timestamp"),
    )

    dedupe_window = Window.partitionBy(*PATIENT_DEMO_KEY_COLUMNS).orderBy(
        F.col("_load_timestamp").desc_nulls_last(),
        F.col("_ingest_batch_id").desc_nulls_last(),
    )

    return (
        transformed_df.withColumn("_row_num", F.row_number().over(dedupe_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_ingest_batch_id", "_load_timestamp")
    )


def write_patient_demo_delta(curated_df: "DataFrame", output_path: str) -> None:
    from delta.tables import DeltaTable

    spark = curated_df.sparkSession
    merge_condition = (
        "target.safetyreportid = source.safetyreportid "
        "AND target.safetyreportversion = source.safetyreportversion"
    )

    if DeltaTable.isDeltaTable(spark, output_path):
        (
            DeltaTable.forPath(spark, output_path)
            .alias("target")
            .merge(curated_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    curated_df.write.format("delta").mode("overwrite").save(output_path)


def run_patient_demo_job(
    spark: "SparkSession",
    raw_input_path: str,
    output_path: str,
) -> CuratedPatientDemoJobResult:
    raw_df = spark.read.json(raw_input_path)
    curated_df = transform_patient_demo_df(raw_df)
    records_written = curated_df.count()
    write_patient_demo_delta(curated_df, output_path=output_path)

    logger.info(
        "Wrote %s records to %s table=%s",
        records_written,
        output_path,
        CURATED_PATIENT_DEMO_TABLE,
    )
    return CuratedPatientDemoJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated_patient_demo from raw bronze envelopes.")
    add_common_databricks_args(parser)
    parser.add_argument("--raw-input-path", required=True, help="Path to raw bronze NDJSON envelopes.")
    parser.add_argument("--output-path", required=True, help="Delta output path for curated_patient_demo.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)

    spark = SparkSession.builder.appName("build_patient_demo").getOrCreate()
    result = run_patient_demo_job(
        spark=spark,
        raw_input_path=args.raw_input_path,
        output_path=args.output_path,
    )
    logger.info("Job complete: %s", result.to_dict())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
