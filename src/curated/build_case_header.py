from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping

import sys

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
from src.common.normalization import as_text as _as_text

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CURATED_CASE_HEADER_TABLE = "curated_case_header"
CASE_HEADER_KEY_COLUMNS = ("safetyreportid", "safetyreportversion")
CASE_HEADER_PARTITION_COLUMNS = ("report_year", "report_month")

_TRUTHY_YN_CODES = {"1", 1, True}


@dataclass(frozen=True)
class CuratedCaseHeaderJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_openfda_date(value: Any) -> date | None:
    text_value = _as_text(value)
    if text_value is None:
        return None
    try:
        return datetime.strptime(text_value, "%Y%m%d").date()
    except ValueError:
        logger.warning("Unable to parse openFDA date value=%s", text_value)
        return None


def yn_code_to_flag(value: Any) -> int:
    return 1 if value in _TRUTHY_YN_CODES else 0


def derive_duplicate_flag(raw_payload: Mapping[str, Any]) -> int:
    if raw_payload.get("duplicate") in _TRUTHY_YN_CODES:
        return 1
    if raw_payload.get("reportduplicate") not in (None, "", {}):
        return 1
    return 0


def derive_occur_country(raw_payload: Mapping[str, Any]) -> str | None:
    primary_source = raw_payload.get("primarysource") or {}
    return (
        _as_text(raw_payload.get("occurcountry"))
        or _as_text(raw_payload.get("primarysourcecountry"))
        or _as_text(primary_source.get("reportercountry"))
    )


def derive_report_partitions(report_date: date | None) -> tuple[int | None, int | None, int | None]:
    if report_date is None:
        return None, None, None
    quarter = ((report_date.month - 1) // 3) + 1
    return report_date.year, report_date.month, quarter


def normalize_case_header_envelope(raw_envelope: Mapping[str, Any]) -> dict[str, Any]:
    raw_payload = raw_envelope.get("raw_payload") or {}
    receipt_date = parse_openfda_date(raw_payload.get("receiptdate"))
    receive_date = parse_openfda_date(raw_payload.get("receivedate"))
    report_date = receipt_date or receive_date

    serious_flag = yn_code_to_flag(raw_payload.get("serious"))
    seriousness_death_flag = yn_code_to_flag(raw_payload.get("seriousnessdeath"))
    seriousness_hospitalization_flag = yn_code_to_flag(raw_payload.get("seriousnesshospitalization"))
    seriousness_lifethreatening_flag = yn_code_to_flag(raw_payload.get("seriousnesslifethreatening"))
    seriousness_disabling_flag = yn_code_to_flag(raw_payload.get("seriousnessdisabling"))
    seriousness_congenital_anomaly_flag = yn_code_to_flag(
        raw_payload.get("seriousnesscongenitalanomali")
    )
    seriousness_other_flag = yn_code_to_flag(raw_payload.get("seriousnessother"))

    report_year, report_month, report_quarter = derive_report_partitions(report_date)
    serious_case_ind = max(
        serious_flag,
        seriousness_death_flag,
        seriousness_hospitalization_flag,
        seriousness_lifethreatening_flag,
        seriousness_disabling_flag,
        seriousness_congenital_anomaly_flag,
        seriousness_other_flag,
    )

    return {
        "safetyreportid": _as_text(raw_envelope.get("safetyreportid") or raw_payload.get("safetyreportid")),
        "safetyreportversion": _as_text(
            raw_envelope.get("safetyreportversion") or raw_payload.get("safetyreportversion")
        ),
        "companynumb": _as_text(raw_payload.get("companynumb")),
        "duplicate_flag": derive_duplicate_flag(raw_payload),
        "occur_country": derive_occur_country(raw_payload),
        "receipt_date": receipt_date.isoformat() if receipt_date else None,
        "receive_date": receive_date.isoformat() if receive_date else None,
        "report_type_code": _as_text(raw_payload.get("reporttype")),
        "serious_flag": serious_flag,
        "seriousness_death_flag": seriousness_death_flag,
        "seriousness_hospitalization_flag": seriousness_hospitalization_flag,
        "seriousness_lifethreatening_flag": seriousness_lifethreatening_flag,
        "seriousness_disabling_flag": seriousness_disabling_flag,
        "seriousness_congenital_anomaly_flag": seriousness_congenital_anomaly_flag,
        "seriousness_other_flag": seriousness_other_flag,
        "ingest_batch_id": _as_text(raw_envelope.get("ingest_batch_id")),
        "source_file_name": _as_text(raw_envelope.get("source_file_name")),
        "load_timestamp": _as_text(raw_envelope.get("load_timestamp")),
        "report_year": report_year,
        "report_month": report_month,
        "report_quarter": report_quarter,
        "serious_case_ind": serious_case_ind,
    }


def _sort_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("load_timestamp")) or "",
        _as_text(record.get("ingest_batch_id")) or "",
    )


def deduplicate_case_header_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
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


def build_case_header_records(raw_envelopes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized_records = [normalize_case_header_envelope(raw_envelope) for raw_envelope in raw_envelopes]
    return deduplicate_case_header_records(normalized_records)


def load_raw_envelopes_from_ndjson(path: str | Path) -> list[dict[str, Any]]:
    resolved_path = Path(path).expanduser()
    with resolved_path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _spark_flag_expr(column: "Any") -> "Any":
    from pyspark.sql import functions as F

    return F.when(column == F.lit("1"), F.lit(1)).otherwise(F.lit(0))


def transform_case_header_df(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    receipt_date_expr = F.to_date(F.col("raw_payload.receiptdate"), "yyyyMMdd")
    receive_date_expr = F.to_date(F.col("raw_payload.receivedate"), "yyyyMMdd")
    report_date_expr = F.coalesce(receipt_date_expr, receive_date_expr)

    serious_flag_expr = _spark_flag_expr(F.col("raw_payload.serious"))
    seriousness_death_flag_expr = _spark_flag_expr(F.col("raw_payload.seriousnessdeath"))
    seriousness_hospitalization_flag_expr = _spark_flag_expr(F.col("raw_payload.seriousnesshospitalization"))
    seriousness_lifethreatening_flag_expr = _spark_flag_expr(
        F.col("raw_payload.seriousnesslifethreatening")
    )
    seriousness_disabling_flag_expr = _spark_flag_expr(F.col("raw_payload.seriousnessdisabling"))
    seriousness_congenital_anomaly_flag_expr = _spark_flag_expr(
        F.col("raw_payload.seriousnesscongenitalanomali")
    )
    seriousness_other_flag_expr = _spark_flag_expr(F.col("raw_payload.seriousnessother"))

    duplicate_flag_expr = F.when(
        (F.col("raw_payload.duplicate") == F.lit("1")) | F.col("raw_payload.reportduplicate").isNotNull(),
        F.lit(1),
    ).otherwise(F.lit(0))

    transformed_df = raw_df.select(
        F.coalesce(F.col("safetyreportid"), F.col("raw_payload.safetyreportid")).alias("safetyreportid"),
        F.coalesce(F.col("safetyreportversion"), F.col("raw_payload.safetyreportversion")).alias(
            "safetyreportversion"
        ),
        F.col("raw_payload.companynumb").cast("string").alias("companynumb"),
        duplicate_flag_expr.alias("duplicate_flag"),
        F.coalesce(
            F.col("raw_payload.occurcountry"),
            F.col("raw_payload.primarysourcecountry"),
            F.col("raw_payload.primarysource.reportercountry"),
        )
        .cast("string")
        .alias("occur_country"),
        receipt_date_expr.alias("receipt_date"),
        receive_date_expr.alias("receive_date"),
        F.col("raw_payload.reporttype").cast("string").alias("report_type_code"),
        serious_flag_expr.alias("serious_flag"),
        seriousness_death_flag_expr.alias("seriousness_death_flag"),
        seriousness_hospitalization_flag_expr.alias("seriousness_hospitalization_flag"),
        seriousness_lifethreatening_flag_expr.alias("seriousness_lifethreatening_flag"),
        seriousness_disabling_flag_expr.alias("seriousness_disabling_flag"),
        seriousness_congenital_anomaly_flag_expr.alias("seriousness_congenital_anomaly_flag"),
        seriousness_other_flag_expr.alias("seriousness_other_flag"),
        F.col("ingest_batch_id").cast("string").alias("ingest_batch_id"),
        F.col("source_file_name").cast("string").alias("source_file_name"),
        F.col("load_timestamp").cast("timestamp").alias("load_timestamp"),
        F.year(report_date_expr).alias("report_year"),
        F.month(report_date_expr).alias("report_month"),
        F.quarter(report_date_expr).alias("report_quarter"),
        F.greatest(
            serious_flag_expr,
            seriousness_death_flag_expr,
            seriousness_hospitalization_flag_expr,
            seriousness_lifethreatening_flag_expr,
            seriousness_disabling_flag_expr,
            seriousness_congenital_anomaly_flag_expr,
            seriousness_other_flag_expr,
        ).alias("serious_case_ind"),
    )

    dedupe_window = Window.partitionBy(*CASE_HEADER_KEY_COLUMNS).orderBy(
        F.col("load_timestamp").desc_nulls_last(),
        F.col("ingest_batch_id").desc_nulls_last(),
    )

    return (
        transformed_df.withColumn("_row_num", F.row_number().over(dedupe_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def write_case_header_delta(curated_df: "DataFrame", output_path: str) -> None:
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

    (
        curated_df.write.format("delta")
        .mode("overwrite")
        .partitionBy(*CASE_HEADER_PARTITION_COLUMNS)
        .save(output_path)
    )


def run_case_header_job(
    spark: "SparkSession",
    raw_input_path: str,
    output_path: str,
) -> CuratedCaseHeaderJobResult:
    raw_df = spark.read.json(raw_input_path)
    curated_df = transform_case_header_df(raw_df)
    records_written = curated_df.count()
    write_case_header_delta(curated_df, output_path=output_path)

    logger.info(
        "Wrote %s records to %s table=%s",
        records_written,
        output_path,
        CURATED_CASE_HEADER_TABLE,
    )
    return CuratedCaseHeaderJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated_case_header from raw bronze envelopes.")
    add_common_databricks_args(parser)
    parser.add_argument("--raw-input-path", required=True, help="Path to raw bronze NDJSON envelopes.")
    parser.add_argument("--output-path", required=True, help="Delta output path for curated_case_header.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)

    spark = SparkSession.builder.appName("build_case_header").getOrCreate()
    result = run_case_header_job(
        spark=spark,
        raw_input_path=args.raw_input_path,
        output_path=args.output_path,
    )
    logger.info("Job complete: %s", result.to_dict())

    return 0


if __name__ == "__main__":
    main()
