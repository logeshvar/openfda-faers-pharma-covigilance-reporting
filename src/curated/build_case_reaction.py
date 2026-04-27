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
from src.common.normalization import as_list as _as_list
from src.common.normalization import as_text as _as_text

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CURATED_CASE_REACTION_TABLE = "curated_case_reaction"
CASE_REACTION_KEY_COLUMNS = ("safetyreportid", "safetyreportversion", "reaction_seq_num")
CASE_REACTION_PARTITION_COLUMNS: tuple[str, ...] = ()

REACTION_OUTCOME_LABELS = {
    "1": "Recovered/Resolved",
    "2": "Recovering/Resolving",
    "3": "Not Recovered/Not Resolved",
    "4": "Recovered/Resolved With Sequelae",
    "5": "Fatal",
    "6": "Unknown",
}


@dataclass(frozen=True)
class CuratedCaseReactionJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def reaction_outcome_label(code: Any) -> str | None:
    text_code = _as_text(code)
    if text_code is None:
        return None
    return REACTION_OUTCOME_LABELS.get(text_code, "Unknown")


def normalize_case_reaction_envelope(raw_envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_payload = raw_envelope.get("raw_payload") or {}
    patient = raw_payload.get("patient") or {}
    rows: list[dict[str, Any]] = []

    for index, reaction in enumerate(_as_list(patient.get("reaction")), start=1):
        if not isinstance(reaction, Mapping):
            continue
        outcome_code = _as_text(reaction.get("reactionoutcome"))
        rows.append(
            {
                "safetyreportid": _as_text(
                    raw_envelope.get("safetyreportid") or raw_payload.get("safetyreportid")
                ),
                "safetyreportversion": _as_text(
                    raw_envelope.get("safetyreportversion") or raw_payload.get("safetyreportversion")
                ),
                "reaction_seq_num": index,
                "reaction_meddra_pt": _as_text(reaction.get("reactionmeddrapt")),
                "reaction_outcome_code": outcome_code,
                "reaction_outcome_label": reaction_outcome_label(outcome_code),
                "_ingest_batch_id": _as_text(raw_envelope.get("ingest_batch_id")),
                "_load_timestamp": _as_text(raw_envelope.get("load_timestamp")),
            }
        )
    return rows


def _sort_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (_as_text(record.get("_load_timestamp")) or "", _as_text(record.get("_ingest_batch_id")) or "")


def build_case_reaction_records(raw_envelopes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str | None, str | None, int | None], dict[str, Any]] = {}
    rows = [row for envelope in raw_envelopes for row in normalize_case_reaction_envelope(envelope)]

    for row in rows:
        key = (row.get("safetyreportid"), row.get("safetyreportversion"), row.get("reaction_seq_num"))
        existing = latest_by_key.get(key)
        if existing is None or _sort_key(row) >= _sort_key(existing):
            latest_by_key[key] = row

    return [
        {key: value for key, value in record.items() if not key.startswith("_")}
        for record in latest_by_key.values()
    ]


def transform_case_reaction_df(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    exploded_df = raw_df.select(
        F.coalesce(F.col("safetyreportid"), F.col("raw_payload.safetyreportid")).alias("safetyreportid"),
        F.coalesce(F.col("safetyreportversion"), F.col("raw_payload.safetyreportversion")).alias(
            "safetyreportversion"
        ),
        F.col("ingest_batch_id").cast("string").alias("_ingest_batch_id"),
        F.col("load_timestamp").cast("timestamp").alias("_load_timestamp"),
        F.posexplode(F.col("raw_payload.patient.reaction")).alias("_reaction_index", "reaction"),
    )
    outcome_code_col = F.col("reaction.reactionoutcome").cast("string")

    transformed_df = exploded_df.select(
        "safetyreportid",
        "safetyreportversion",
        (F.col("_reaction_index") + F.lit(1)).alias("reaction_seq_num"),
        F.col("reaction.reactionmeddrapt").cast("string").alias("reaction_meddra_pt"),
        outcome_code_col.alias("reaction_outcome_code"),
        F.when(outcome_code_col == F.lit("1"), F.lit("Recovered/Resolved"))
        .when(outcome_code_col == F.lit("2"), F.lit("Recovering/Resolving"))
        .when(outcome_code_col == F.lit("3"), F.lit("Not Recovered/Not Resolved"))
        .when(outcome_code_col == F.lit("4"), F.lit("Recovered/Resolved With Sequelae"))
        .when(outcome_code_col == F.lit("5"), F.lit("Fatal"))
        .when(outcome_code_col == F.lit("6"), F.lit("Unknown"))
        .when(outcome_code_col.isNull(), F.lit(None).cast("string"))
        .otherwise(F.lit("Unknown"))
        .alias("reaction_outcome_label"),
        "_ingest_batch_id",
        "_load_timestamp",
    )

    dedupe_window = Window.partitionBy(*CASE_REACTION_KEY_COLUMNS).orderBy(
        F.col("_load_timestamp").desc_nulls_last(),
        F.col("_ingest_batch_id").desc_nulls_last(),
    )

    return (
        transformed_df.withColumn("_row_num", F.row_number().over(dedupe_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_ingest_batch_id", "_load_timestamp")
    )


def write_case_reaction_delta(curated_df: "DataFrame", output_path: str) -> None:
    from delta.tables import DeltaTable

    spark = curated_df.sparkSession
    merge_condition = (
        "target.safetyreportid = source.safetyreportid "
        "AND target.safetyreportversion = source.safetyreportversion "
        "AND target.reaction_seq_num = source.reaction_seq_num"
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


def run_case_reaction_job(
    spark: "SparkSession", raw_input_path: str, output_path: str
) -> CuratedCaseReactionJobResult:
    raw_df = spark.read.json(raw_input_path)
    curated_df = transform_case_reaction_df(raw_df)
    records_written = curated_df.count()
    write_case_reaction_delta(curated_df, output_path=output_path)
    return CuratedCaseReactionJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated_case_reaction from raw bronze envelopes.")
    add_common_databricks_args(parser)
    parser.add_argument("--raw-input-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_case_reaction").getOrCreate()
    logger.info(
        "Job complete: %s",
        run_case_reaction_job(spark, args.raw_input_path, args.output_path).to_dict(),
    )
    return 0


if __name__ == "__main__":
    main()
