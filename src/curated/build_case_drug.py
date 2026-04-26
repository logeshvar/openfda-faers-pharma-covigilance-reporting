from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from src.common.normalization import as_list as _as_list
from src.common.normalization import as_text as _as_text

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CURATED_CASE_DRUG_TABLE = "curated_case_drug"
CASE_DRUG_KEY_COLUMNS = ("safetyreportid", "safetyreportversion", "drug_seq_num")
CASE_DRUG_PARTITION_COLUMNS: tuple[str, ...] = ()

DRUG_CHARACTERIZATION_LABELS = {
    "1": "Suspect",
    "2": "Concomitant",
    "3": "Interacting",
}

ACTIONDRUG_LABELS = {
    "1": "Drug Withdrawn",
    "2": "Dose Reduced",
    "3": "Dose Increased",
    "4": "Dose Not Changed",
    "5": "Unknown",
    "6": "Not Applicable",
}


@dataclass(frozen=True)
class CuratedCaseDrugJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def drug_characterization_label(code: Any) -> str | None:
    text_code = _as_text(code)
    if text_code is None:
        return None
    return DRUG_CHARACTERIZATION_LABELS.get(text_code, "Unknown")


def actiondrug_label(code: Any) -> str | None:
    text_code = _as_text(code)
    if text_code is None:
        return None
    return ACTIONDRUG_LABELS.get(text_code, "Unknown")


def normalize_case_drug_envelope(raw_envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_payload = raw_envelope.get("raw_payload") or {}
    patient = raw_payload.get("patient") or {}
    drugs = _as_list(patient.get("drug"))
    rows: list[dict[str, Any]] = []

    for index, drug in enumerate(drugs, start=1):
        if not isinstance(drug, Mapping):
            continue

        active_substance = drug.get("activesubstance") or {}
        characterization_code = _as_text(drug.get("drugcharacterization"))
        action_code = _as_text(drug.get("actiondrug"))

        rows.append(
            {
                "safetyreportid": _as_text(
                    raw_envelope.get("safetyreportid") or raw_payload.get("safetyreportid")
                ),
                "safetyreportversion": _as_text(
                    raw_envelope.get("safetyreportversion") or raw_payload.get("safetyreportversion")
                ),
                "drug_seq_num": index,
                "medicinal_product": _as_text(drug.get("medicinalproduct")),
                "active_substance_name": _as_text(active_substance.get("activesubstancename")),
                "drug_characterization_code": characterization_code,
                "drug_characterization_label": drug_characterization_label(characterization_code),
                "drug_indication": _as_text(drug.get("drugindication")),
                "actiondrug_code": action_code,
                "actiondrug_label": actiondrug_label(action_code),
                "drug_administration_route_code": _as_text(drug.get("drugadministrationroute")),
                "_ingest_batch_id": _as_text(raw_envelope.get("ingest_batch_id")),
                "_load_timestamp": _as_text(raw_envelope.get("load_timestamp")),
            }
        )

    return rows


def _sort_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("_load_timestamp")) or "",
        _as_text(record.get("_ingest_batch_id")) or "",
    )


def deduplicate_case_drug_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str | None, str | None, int | None], dict[str, Any]] = {}

    for record in records:
        normalized_record = dict(record)
        key = (
            _as_text(normalized_record.get("safetyreportid")),
            _as_text(normalized_record.get("safetyreportversion")),
            normalized_record.get("drug_seq_num"),
        )
        existing_record = latest_by_key.get(key)
        if existing_record is None or _sort_key(normalized_record) >= _sort_key(existing_record):
            latest_by_key[key] = normalized_record

    return [
        {key: value for key, value in record.items() if not key.startswith("_")}
        for record in latest_by_key.values()
    ]


def build_case_drug_records(raw_envelopes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for raw_envelope in raw_envelopes for row in normalize_case_drug_envelope(raw_envelope)]
    return deduplicate_case_drug_records(rows)


def transform_case_drug_df(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    exploded_df = raw_df.select(
        F.coalesce(F.col("safetyreportid"), F.col("raw_payload.safetyreportid")).alias("safetyreportid"),
        F.coalesce(F.col("safetyreportversion"), F.col("raw_payload.safetyreportversion")).alias(
            "safetyreportversion"
        ),
        F.col("ingest_batch_id").cast("string").alias("_ingest_batch_id"),
        F.col("load_timestamp").cast("timestamp").alias("_load_timestamp"),
        F.posexplode(F.col("raw_payload.patient.drug")).alias("_drug_index", "drug"),
    )

    characterization_code_col = F.col("drug.drugcharacterization").cast("string")
    action_code_col = F.col("drug.actiondrug").cast("string")

    transformed_df = exploded_df.select(
        "safetyreportid",
        "safetyreportversion",
        (F.col("_drug_index") + F.lit(1)).alias("drug_seq_num"),
        F.col("drug.medicinalproduct").cast("string").alias("medicinal_product"),
        F.col("drug.activesubstance.activesubstancename").cast("string").alias("active_substance_name"),
        characterization_code_col.alias("drug_characterization_code"),
        F.when(characterization_code_col == F.lit("1"), F.lit("Suspect"))
        .when(characterization_code_col == F.lit("2"), F.lit("Concomitant"))
        .when(characterization_code_col == F.lit("3"), F.lit("Interacting"))
        .when(characterization_code_col.isNull(), F.lit(None).cast("string"))
        .otherwise(F.lit("Unknown"))
        .alias("drug_characterization_label"),
        F.col("drug.drugindication").cast("string").alias("drug_indication"),
        action_code_col.alias("actiondrug_code"),
        F.when(action_code_col == F.lit("1"), F.lit("Drug Withdrawn"))
        .when(action_code_col == F.lit("2"), F.lit("Dose Reduced"))
        .when(action_code_col == F.lit("3"), F.lit("Dose Increased"))
        .when(action_code_col == F.lit("4"), F.lit("Dose Not Changed"))
        .when(action_code_col == F.lit("5"), F.lit("Unknown"))
        .when(action_code_col == F.lit("6"), F.lit("Not Applicable"))
        .when(action_code_col.isNull(), F.lit(None).cast("string"))
        .otherwise(F.lit("Unknown"))
        .alias("actiondrug_label"),
        F.col("drug.drugadministrationroute").cast("string").alias("drug_administration_route_code"),
        "_ingest_batch_id",
        "_load_timestamp",
    )

    dedupe_window = Window.partitionBy(*CASE_DRUG_KEY_COLUMNS).orderBy(
        F.col("_load_timestamp").desc_nulls_last(),
        F.col("_ingest_batch_id").desc_nulls_last(),
    )

    return (
        transformed_df.withColumn("_row_num", F.row_number().over(dedupe_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_ingest_batch_id", "_load_timestamp")
    )


def write_case_drug_delta(curated_df: "DataFrame", output_path: str) -> None:
    from delta.tables import DeltaTable

    spark = curated_df.sparkSession
    merge_condition = (
        "target.safetyreportid = source.safetyreportid "
        "AND target.safetyreportversion = source.safetyreportversion "
        "AND target.drug_seq_num = source.drug_seq_num"
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


def run_case_drug_job(spark: "SparkSession", raw_input_path: str, output_path: str) -> CuratedCaseDrugJobResult:
    raw_df = spark.read.json(raw_input_path)
    curated_df = transform_case_drug_df(raw_df)
    records_written = curated_df.count()
    write_case_drug_delta(curated_df, output_path=output_path)
    return CuratedCaseDrugJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated_case_drug from raw bronze envelopes.")
    parser.add_argument("--raw-input-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = SparkSession.builder.appName("build_case_drug").getOrCreate()
    try:
        logger.info("Job complete: %s", run_case_drug_job(spark, args.raw_input_path, args.output_path).to_dict())
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
