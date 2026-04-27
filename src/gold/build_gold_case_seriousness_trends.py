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
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

GOLD_CASE_SERIOUSNESS_TRENDS_TABLE = "gold_case_seriousness_trends"


@dataclass(frozen=True)
class GoldJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_gold_case_seriousness_trends_df(latest_case_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return latest_case_df.groupBy("report_year", "report_month", "report_quarter").agg(
        F.countDistinct("safetyreportid").alias("case_count"),
        F.sum("serious_case_ind").alias("serious_case_count"),
        (F.countDistinct("safetyreportid") - F.sum("serious_case_ind")).alias("non_serious_case_count"),
        F.sum("seriousness_death_flag").alias("death_case_count"),
        F.sum("seriousness_hospitalization_flag").alias("hospitalization_case_count"),
        F.sum("seriousness_lifethreatening_flag").alias("lifethreatening_case_count"),
        F.sum("seriousness_disabling_flag").alias("disabling_case_count"),
        F.sum("seriousness_congenital_anomaly_flag").alias("congenital_anomaly_case_count"),
        F.sum("seriousness_other_flag").alias("other_seriousness_case_count"),
    )


def run_gold_case_seriousness_trends_job(
    spark: "SparkSession", latest_case_path: str, output_path: str
) -> GoldJobResult:
    latest_case_df = spark.read.format("delta").load(latest_case_path)
    gold_df = build_gold_case_seriousness_trends_df(latest_case_df)
    records_written = gold_df.count()
    gold_df.write.format("delta").mode("overwrite").partitionBy("report_year", "report_month").save(output_path)
    return GoldJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_case_seriousness_trends.")
    add_common_databricks_args(parser)
    parser.add_argument("--latest-case-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_gold_case_seriousness_trends").getOrCreate()
    try:
        logger.info(
            "Job complete: %s",
            run_gold_case_seriousness_trends_job(spark, args.latest_case_path, args.output_path).to_dict(),
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
