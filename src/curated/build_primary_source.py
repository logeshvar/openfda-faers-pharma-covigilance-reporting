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
from src.common.normalization import as_text as _as_text

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CURATED_PRIMARY_SOURCE_TABLE = "curated_primary_source"
PRIMARY_SOURCE_KEY_COLUMNS = ("safetyreportid", "safetyreportversion")
PRIMARY_SOURCE_PARTITION_COLUMNS: tuple[str, ...] = ()

QUALIFICATION_LABELS = {
    "1": "Physician",
    "2": "Pharmacist",
    "3": "Other Health Professional",
    "4": "Lawyer",
    "5": "Consumer or Non-Health Professional",
}


@dataclass(frozen=True)
class CuratedPrimarySourceJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def qualification_label(qualification_code: Any) -> str | None:
    code = _as_text(qualification_code)
    if code is None:
        return None
    return QUALIFICATION_LABELS.get(code, "Unknown")


def normalize_primary_source_envelope(raw_envelope: Mapping[str, Any]) -> dict[str, Any]:
    raw_payload = raw_envelope.get("raw_payload") or {}
    primary_source = raw_payload.get("primarysource") or {}
    qualification_code = _as_text(primary_source.get("qualification"))

    return {
        "safetyreportid": _as_text(raw_envelope.get("safetyreportid") or raw_payload.get("safetyreportid")),
        "safetyreportversion": _as_text(
            raw_envelope.get("safetyreportversion") or raw_payload.get("safetyreportversion")
        ),
        "qualification_code": qualification_code,
        "qualification_label": qualification_label(qualification_code),
        "reporter_country": _as_text(primary_source.get("reportercountry")),
    }


def _sort_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("load_timestamp")) or "",
        _as_text(record.get("ingest_batch_id")) or "",
    )


def deduplicate_primary_source_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
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


def build_primary_source_records(raw_envelopes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized_records = [normalize_primary_source_envelope(raw_envelope) for raw_envelope in raw_envelopes]
    return deduplicate_primary_source_records(normalized_records)


def transform_primary_source_df(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    qualification_code_col = F.col("raw_payload.primarysource.qualification").cast("string")
    qualification_label_expr = (
        F.when(qualification_code_col == F.lit("1"), F.lit("Physician"))
        .when(qualification_code_col == F.lit("2"), F.lit("Pharmacist"))
        .when(qualification_code_col == F.lit("3"), F.lit("Other Health Professional"))
        .when(qualification_code_col == F.lit("4"), F.lit("Lawyer"))
        .when(qualification_code_col == F.lit("5"), F.lit("Consumer or Non-Health Professional"))
        .when(qualification_code_col.isNull(), F.lit(None).cast("string"))
        .otherwise(F.lit("Unknown"))
    )

    transformed_df = raw_df.select(
        F.coalesce(F.col("safetyreportid"), F.col("raw_payload.safetyreportid")).alias("safetyreportid"),
        F.coalesce(F.col("safetyreportversion"), F.col("raw_payload.safetyreportversion")).alias(
            "safetyreportversion"
        ),
        qualification_code_col.alias("qualification_code"),
        qualification_label_expr.alias("qualification_label"),
        F.col("raw_payload.primarysource.reportercountry").cast("string").alias("reporter_country"),
        F.col("ingest_batch_id").cast("string").alias("_ingest_batch_id"),
        F.col("load_timestamp").cast("timestamp").alias("_load_timestamp"),
    )

    dedupe_window = Window.partitionBy(*PRIMARY_SOURCE_KEY_COLUMNS).orderBy(
        F.col("_load_timestamp").desc_nulls_last(),
        F.col("_ingest_batch_id").desc_nulls_last(),
    )

    return (
        transformed_df.withColumn("_row_num", F.row_number().over(dedupe_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_ingest_batch_id", "_load_timestamp")
    )


def write_primary_source_delta(curated_df: "DataFrame", output_path: str) -> None:
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


def run_primary_source_job(
    spark: "SparkSession",
    raw_input_path: str,
    output_path: str,
) -> CuratedPrimarySourceJobResult:
    raw_df = spark.read.json(raw_input_path)
    curated_df = transform_primary_source_df(raw_df)
    records_written = curated_df.count()
    write_primary_source_delta(curated_df, output_path=output_path)

    logger.info(
        "Wrote %s records to %s table=%s",
        records_written,
        output_path,
        CURATED_PRIMARY_SOURCE_TABLE,
    )
    return CuratedPrimarySourceJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated_primary_source from raw bronze envelopes.")
    add_common_databricks_args(parser)
    parser.add_argument("--raw-input-path", required=True, help="Path to raw bronze NDJSON envelopes.")
    parser.add_argument("--output-path", required=True, help="Delta output path for curated_primary_source.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)

    spark = SparkSession.builder.appName("build_primary_source").getOrCreate()
    result = run_primary_source_job(
        spark=spark,
        raw_input_path=args.raw_input_path,
        output_path=args.output_path,
    )
    logger.info("Job complete: %s", result.to_dict())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
