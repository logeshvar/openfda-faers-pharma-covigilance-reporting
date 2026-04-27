from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from pathlib import Path
import sys

for _parent in Path(__file__).resolve().parents:
    if (_parent / "src").is_dir() and (_parent / "conf").is_dir():
        sys.path.insert(0, str(_parent))
        break

from src.common.databricks_runtime import add_common_databricks_args, log_common_databricks_args

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

GOLD_DRUG_REACTION_TRENDS_TABLE = "gold_drug_reaction_trends"


@dataclass(frozen=True)
class GoldJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_gold_drug_reaction_trends_job(
    spark: "SparkSession",
    latest_case_path: str,
    case_drug_path: str,
    case_reaction_path: str,
    output_path: str,
) -> GoldJobResult:
    from pyspark.sql import functions as F

    latest = spark.read.format("delta").load(latest_case_path)
    drugs = spark.read.format("delta").load(case_drug_path)
    reactions = spark.read.format("delta").load(case_reaction_path)

    joined = (
        latest.join(drugs, ["safetyreportid", "safetyreportversion"], "inner")
        .join(reactions, ["safetyreportid", "safetyreportversion"], "inner")
    )

    gold_df = joined.groupBy(
        "report_year",
        "report_month",
        "report_quarter",
        F.upper(F.coalesce("medicinal_product", "active_substance_name")).alias("drug_name"),
        "reaction_meddra_pt",
    ).agg(
        F.countDistinct("safetyreportid").alias("case_count"),
        F.sum("serious_case_ind").alias("serious_case_count"),
    )
    records_written = gold_df.count()
    gold_df.write.format("delta").mode("overwrite").partitionBy("report_year", "report_month").save(output_path)
    return GoldJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_drug_reaction_trends.")
    add_common_databricks_args(parser)
    parser.add_argument("--latest-case-path", required=True)
    parser.add_argument("--case-drug-path", required=True)
    parser.add_argument("--case-reaction-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_gold_drug_reaction_trends").getOrCreate()
    try:
        logger.info(
            "Job complete: %s",
            run_gold_drug_reaction_trends_job(
                spark, args.latest_case_path, args.case_drug_path, args.case_reaction_path, args.output_path
            ).to_dict(),
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
