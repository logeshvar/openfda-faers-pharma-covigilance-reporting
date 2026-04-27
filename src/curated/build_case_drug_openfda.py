from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping

import sys
from pathlib import Path

for _parent in Path(__file__).resolve().parents:
    if (_parent / "src").is_dir() and (_parent / "conf").is_dir():
        sys.path.insert(0, str(_parent))
        break

from src.common.databricks_runtime import add_common_databricks_args, log_common_databricks_args
from src.common.normalization import as_list as _as_list
from src.common.normalization import as_text as _as_text

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CURATED_CASE_DRUG_OPENFDA_TABLE = "curated_case_drug_openfda"
CASE_DRUG_OPENFDA_KEY_COLUMNS = ("safetyreportid", "safetyreportversion", "drug_seq_num")
CASE_DRUG_OPENFDA_PARTITION_COLUMNS: tuple[str, ...] = ()


@dataclass(frozen=True)
class CuratedCaseDrugOpenFDAJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_case_drug_openfda_envelope(raw_envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_payload = raw_envelope.get("raw_payload") or {}
    patient = raw_payload.get("patient") or {}
    rows: list[dict[str, Any]] = []

    for index, drug in enumerate(_as_list(patient.get("drug")), start=1):
        if not isinstance(drug, Mapping):
            continue
        openfda = drug.get("openfda") or {}
        rows.append(
            {
                "safetyreportid": _as_text(
                    raw_envelope.get("safetyreportid") or raw_payload.get("safetyreportid")
                ),
                "safetyreportversion": _as_text(
                    raw_envelope.get("safetyreportversion") or raw_payload.get("safetyreportversion")
                ),
                "drug_seq_num": index,
                "generic_name_arr": [_as_text(value) for value in _as_list(openfda.get("generic_name"))],
                "brand_name_arr": [_as_text(value) for value in _as_list(openfda.get("brand_name"))],
                "manufacturer_name_arr": [
                    _as_text(value) for value in _as_list(openfda.get("manufacturer_name"))
                ],
                "pharm_class_epc_arr": [_as_text(value) for value in _as_list(openfda.get("pharm_class_epc"))],
                "substance_name_arr": [_as_text(value) for value in _as_list(openfda.get("substance_name"))],
                "_ingest_batch_id": _as_text(raw_envelope.get("ingest_batch_id")),
                "_load_timestamp": _as_text(raw_envelope.get("load_timestamp")),
            }
        )

    return rows


def _sort_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (_as_text(record.get("_load_timestamp")) or "", _as_text(record.get("_ingest_batch_id")) or "")


def build_case_drug_openfda_records(raw_envelopes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str | None, str | None, int | None], dict[str, Any]] = {}
    rows = [row for envelope in raw_envelopes for row in normalize_case_drug_openfda_envelope(envelope)]

    for row in rows:
        key = (row.get("safetyreportid"), row.get("safetyreportversion"), row.get("drug_seq_num"))
        existing = latest_by_key.get(key)
        if existing is None or _sort_key(row) >= _sort_key(existing):
            latest_by_key[key] = row

    return [
        {key: value for key, value in record.items() if not key.startswith("_")}
        for record in latest_by_key.values()
    ]


def transform_case_drug_openfda_df(raw_df: "DataFrame") -> "DataFrame":
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

    transformed_df = exploded_df.select(
        "safetyreportid",
        "safetyreportversion",
        (F.col("_drug_index") + F.lit(1)).alias("drug_seq_num"),
        F.col("drug.openfda.generic_name").alias("generic_name_arr"),
        F.col("drug.openfda.brand_name").alias("brand_name_arr"),
        F.col("drug.openfda.manufacturer_name").alias("manufacturer_name_arr"),
        F.col("drug.openfda.pharm_class_epc").alias("pharm_class_epc_arr"),
        F.col("drug.openfda.substance_name").alias("substance_name_arr"),
        "_ingest_batch_id",
        "_load_timestamp",
    )

    dedupe_window = Window.partitionBy(*CASE_DRUG_OPENFDA_KEY_COLUMNS).orderBy(
        F.col("_load_timestamp").desc_nulls_last(),
        F.col("_ingest_batch_id").desc_nulls_last(),
    )

    return (
        transformed_df.withColumn("_row_num", F.row_number().over(dedupe_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_ingest_batch_id", "_load_timestamp")
    )


def write_case_drug_openfda_delta(curated_df: "DataFrame", output_path: str) -> None:
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


def run_case_drug_openfda_job(
    spark: "SparkSession", raw_input_path: str, output_path: str
) -> CuratedCaseDrugOpenFDAJobResult:
    raw_df = spark.read.json(raw_input_path)
    curated_df = transform_case_drug_openfda_df(raw_df)
    records_written = curated_df.count()
    write_case_drug_openfda_delta(curated_df, output_path=output_path)
    return CuratedCaseDrugOpenFDAJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated_case_drug_openfda from raw bronze envelopes.")
    add_common_databricks_args(parser)
    parser.add_argument("--raw-input-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_case_drug_openfda").getOrCreate()
    try:
        logger.info(
            "Job complete: %s",
            run_case_drug_openfda_job(spark, args.raw_input_path, args.output_path).to_dict(),
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
