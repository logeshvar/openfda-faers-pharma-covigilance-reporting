from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from pathlib import Path
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
from src.common.delta_write import overwrite_report_month_partitions

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

GOLD_REACTION_DEMOGRAPHIC_TRENDS_TABLE = "gold_reaction_demographic_trends"


@dataclass(frozen=True)
class GoldJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_gold_reaction_demographic_trends_job(
    spark: "SparkSession",
    latest_case_path: str,
    reaction_path: str,
    patient_demo_path: str,
    primary_source_path: str,
    output_path: str,
    window_start: str,
    window_end: str,
) -> GoldJobResult:
    from pyspark.sql import functions as F

    latest = spark.read.format("delta").load(latest_case_path)
    reactions = spark.read.format("delta").load(reaction_path)
    demo = spark.read.format("delta").load(patient_demo_path)
    source = spark.read.format("delta").load(primary_source_path)

    joined = (
        latest.join(reactions, ["safetyreportid", "safetyreportversion"], "inner")
        .join(demo, ["safetyreportid", "safetyreportversion"], "left")
        .join(source, ["safetyreportid", "safetyreportversion"], "left")
    )

    gold_df = joined.groupBy(
        "report_year",
        "report_month",
        "report_quarter",
        "reaction_meddra_pt",
        F.coalesce("patientsex_label", F.lit("Unknown")).alias("patientsex_label"),
        F.coalesce("derived_age_band", F.lit("Unknown")).alias("derived_age_band"),
        F.coalesce("reporter_country", F.lit("Unknown")).alias("reporter_country"),
    ).agg(
        F.countDistinct("safetyreportid").alias("case_count"),
        F.sum("serious_case_ind").alias("serious_case_count"),
    )
    records_written = gold_df.count()
    overwrite_report_month_partitions(
        gold_df,
        output_path=output_path,
        window_start=window_start,
        window_end=window_end,
    )
    return GoldJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_reaction_demographic_trends.")
    add_common_databricks_args(parser)
    parser.add_argument("--latest-case-path", required=True)
    parser.add_argument("--reaction-path", required=True)
    parser.add_argument("--patient-demo-path", required=True)
    parser.add_argument("--primary-source-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_gold_reaction_demographic_trends").getOrCreate()
    logger.info(
        "Job complete: %s",
        run_gold_reaction_demographic_trends_job(
            spark,
            args.latest_case_path,
            args.reaction_path,
            args.patient_demo_path,
            args.primary_source_path,
            args.output_path,
            args.window_start,
            args.window_end,
        ).to_dict(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
