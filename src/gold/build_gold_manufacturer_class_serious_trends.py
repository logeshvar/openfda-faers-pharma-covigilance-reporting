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

GOLD_MANUFACTURER_CLASS_SERIOUS_TRENDS_TABLE = "gold_manufacturer_class_serious_trends"


@dataclass(frozen=True)
class GoldJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_gold_manufacturer_class_serious_trends_job(
    spark: "SparkSession",
    latest_case_path: str,
    case_drug_openfda_path: str,
    output_path: str,
) -> GoldJobResult:
    from pyspark.sql import functions as F

    latest = spark.read.format("delta").load(latest_case_path)
    openfda = spark.read.format("delta").load(case_drug_openfda_path)

    joined = latest.join(openfda, ["safetyreportid", "safetyreportversion"], "inner")
    exploded = (
        joined.withColumn("manufacturer_name", F.explode_outer("manufacturer_name_arr"))
        .withColumn("pharm_class_epc", F.explode_outer("pharm_class_epc_arr"))
    )

    gold_df = exploded.groupBy(
        "report_year",
        "report_month",
        "report_quarter",
        F.coalesce("manufacturer_name", F.lit("Unknown")).alias("manufacturer_name"),
        F.coalesce("pharm_class_epc", F.lit("Unknown")).alias("pharm_class_epc"),
    ).agg(
        F.countDistinct("safetyreportid").alias("case_count"),
        F.sum("serious_case_ind").alias("serious_case_count"),
    )
    records_written = gold_df.count()
    gold_df.write.format("delta").mode("overwrite").partitionBy("report_year", "report_month").save(output_path)
    return GoldJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_manufacturer_class_serious_trends.")
    add_common_databricks_args(parser)
    parser.add_argument("--latest-case-path", required=True)
    parser.add_argument("--case-drug-openfda-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_gold_manufacturer_class_serious_trends").getOrCreate()
    logger.info(
        "Job complete: %s",
        run_gold_manufacturer_class_serious_trends_job(
            spark, args.latest_case_path, args.case_drug_openfda_path, args.output_path
        ).to_dict(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
