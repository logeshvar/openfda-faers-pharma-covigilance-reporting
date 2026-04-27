from __future__ import annotations

import argparse
from datetime import datetime, timezone


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Databricks Git job and S3 access.")
    parser.add_argument("--bucket-path", required=True, help="S3 bucket or prefix to list.")
    parser.add_argument("--ops-base-path", required=True, help="S3 ops base path for marker output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    spark = SparkSession.builder.appName("databricks_smoke_test_s3_access").getOrCreate()
    try:
        resolved_dbutils = dbutils  # type: ignore[name-defined] # noqa: F821
    except NameError:
        from pyspark.dbutils import DBUtils

        resolved_dbutils = DBUtils(spark)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    marker_path = args.ops_base_path.rstrip("/") + f"/smoke_tests/databricks_smoke_{timestamp}.txt"

    print(f"Listing bucket_path={args.bucket_path}")
    listing = resolved_dbutils.fs.ls(args.bucket_path)
    print(f"Found {len(listing)} object(s)/prefix(es)")
    for item in listing[:20]:
        print(f"- {item.path}")

    print(f"Writing marker file to {marker_path}")
    resolved_dbutils.fs.put(marker_path, f"Databricks smoke test succeeded at {timestamp}\n", overwrite=True)
    print("Databricks smoke test completed successfully")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
