# Pharma Covigilance Reporting Pipeline

Batch pharmacovigilance reporting pipeline built as a Senior Data Engineer portfolio project.

The project ingests public openFDA drug adverse event reports, lands immutable raw JSON in S3, normalizes the nested case payload into curated Delta tables, builds gold reporting datasets for safety signal monitoring, and registers those outputs in Glue/Athena for SQL access.

This is a reporting and monitoring pipeline. It does not diagnose patients, prove causality, or claim that a drug caused a reaction. Drug and reaction analysis in the gold layer represents co-reporting within the same safety report.

## What This Project Demonstrates

- Batch ingestion from a public API with schedule-aware and manual backfill support
- AWS-style lakehouse layout across raw, ops, curated, and gold S3 zones
- Airflow TaskFlow DAGs for orchestration
- Databricks Jobs API integration using Git-backed Spark Python tasks
- Shared Databricks job cluster with an AWS instance profile for S3 access
- Delta Lake curated and gold outputs
- Glue Catalog registration and Athena querying
- Incremental-friendly reruns with immutable raw batches, curated Delta merges, and partition-aware gold overwrites
- Config-driven local and production-style execution

Optional Bedrock-generated summaries are intentionally left as a future enhancement. The current project focuses on a reliable data engineering foundation first.

## Architecture

The implemented flow is:

1. Airflow triggers `openfda_ingest_raw`.
2. The ingestion code extracts openFDA drug event records for a scheduled or manually supplied date window.
3. Raw records are wrapped in bronze-style envelopes and written as immutable NDJSON to S3.
4. Audit metadata is written to the ops prefix.
5. Airflow triggers a Git-backed Databricks saved job through `openfda_build_curated_gold`.
6. Databricks builds curated Delta tables from the selected raw batch.
7. Databricks builds gold reporting Delta tables from curated data.
8. Airflow runs `openfda_refresh_metadata` to register Delta table locations in Glue through Athena DDL.
9. Athena queries the curated and gold datasets.

Architecture details live in [docs/architecture.md](docs/architecture.md).

## Data Zones

Raw:
- Append-only NDJSON envelopes from openFDA
- Preserves source payload fidelity
- Partitioned by query window and ingest batch

Ops:
- Ingestion audit records
- Athena query result location
- Databricks smoke-test markers

Curated:
- Delta tables normalized from the nested case report payload
- Preserves all report versions using `safetyreportid` and `safetyreportversion`
- Uses Delta merge/upsert behavior once tables exist

Gold:
- Delta reporting tables for trend analysis
- Uses latest report version per `safetyreportid`
- Rebuilds only affected `report_year` and `report_month` partitions

## Tables

Curated tables:
- `curated_case_header`
- `curated_primary_source`
- `curated_patient_demo`
- `curated_case_drug`
- `curated_case_drug_openfda`
- `curated_case_reaction`

Gold tables:
- `gold_latest_case_helper`
- `gold_case_seriousness_trends`
- `gold_drug_reaction_trends`
- `gold_reaction_demographic_trends`
- `gold_manufacturer_class_serious_trends`

Important modeling note: `curated_case_drug` and `curated_case_reaction` are independent child entities under the same report version. The gold drug-reaction trend table is a co-reporting view, not a causality model.

## Repository Layout

```text
airflow/dags/        Airflow DAG definitions
conf/                Environment-specific configuration
docs/                Architecture and design documentation
src/common/          Config, path, AWS, Databricks, and Delta helpers
src/ingestion/       openFDA extraction and raw S3 landing
src/curated/         Curated Spark transformations
src/gold/            Gold Spark transformations
src/dq/              Data quality checks
src/metadata/        Glue/Athena metadata helpers
sql/                 Athena validation and reference SQL
tests/               Unit and helper tests
```

## Local Development

Create a local environment file:

```bash
cp .env.example .env
```

Start the stack:

```bash
docker compose up -d postgres minio minio-create-bucket airflow-init airflow-scheduler airflow-webserver
```

Airflow UI:
- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

MinIO is used only as a local S3-compatible emulator. In AWS-oriented execution, `S3_ENDPOINT_URL` is unset and boto3 uses native Amazon S3.

Confirm the DAGs are available:

```bash
docker compose exec airflow-scheduler airflow dags list | grep openfda_ingest_raw
docker compose exec airflow-scheduler airflow dags list | grep databricks_smoke_test
docker compose exec airflow-scheduler airflow dags list | grep openfda_build_curated_gold
docker compose exec airflow-scheduler airflow dags list | grep openfda_refresh_metadata
```

## Running The Pipeline

Trigger raw ingestion for a one-day window:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_ingest_raw \
  --conf '{"window_start":"2025-12-28","window_end":"2025-12-28","page_size":100,"max_pages":50}'
```

Run the Databricks smoke test before the full build:

```bash
docker compose exec airflow-scheduler airflow dags trigger databricks_smoke_test \
  --conf '{"dry_run":false}'
```

Preview the curated/gold Databricks job payload:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_build_curated_gold \
  --conf '{"window_start":"2025-12-28","window_end":"2025-12-28","dry_run":true}'
```

Submit the curated/gold Databricks job:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_build_curated_gold \
  --conf '{"window_start":"2025-12-28","window_end":"2025-12-28","dry_run":false}'
```

Refresh Glue/Athena metadata:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_refresh_metadata \
  --conf '{"execute_refresh":true,"force_recreate":true}'
```

Use `force_recreate=true` when Glue table definitions need to be recreated while preserving the underlying S3 Delta data.

## Databricks Execution

The production-style path uses Databricks Git source execution, not S3-staged Python files or wheel packaging.

The curated/gold DAG creates or resets a saved Databricks Job. That job uses:
- a top-level `git_source` block
- repo-relative Spark Python files such as `src/curated/build_case_header.py`
- one shared job cluster
- an AWS instance profile for S3 read/write access
- task dependencies that run curated tables in parallel, then gold outputs after the latest-case helper

The job cluster disables Delta deletion vectors so Databricks-written Delta tables remain compatible with Athena's native Delta reader:

```json
{
  "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false"
}
```

Required production-style values:

```env
CV_CONFIG_PATH=/opt/airflow/conf/prod.yaml
S3_BUCKET=pharma-cv-prod
S3_ENDPOINT_URL=
DATABRICKS_SUBMIT_ENABLED=true
DATABRICKS_SECRET_ID=pharma-cv/databricks/prod
DATABRICKS_SUBMISSION_MODE=saved_job
DATABRICKS_JOB_NAME=pharma-cv-curated-gold
DATABRICKS_EXECUTION_SOURCE=git
DATABRICKS_GIT_URL=https://github.com/logeshvar/openfda-faers-pharma-covigilance-reporting.git
DATABRICKS_GIT_PROVIDER=gitHub
DATABRICKS_GIT_BRANCH=main
DATABRICKS_PYTHON_FILE_BASE_PATH=src
DATABRICKS_INSTANCE_PROFILE_ARN=arn:aws:iam::<account-id>:instance-profile/pharma-cv-databricks-role
```

The Databricks API secret in AWS Secrets Manager should contain only:

```json
{
  "host": "https://dbc-xxxx.cloud.databricks.com",
  "token": "your-databricks-token"
}
```

## Scheduling And Backfills

openFDA FAERS data is updated quarterly and may lag by several months. The default scheduled ingestion therefore uses a lagged daily window instead of repeatedly pulling very recent data that may still be incomplete.

Manual backfills are supported with `window_start` and `window_end`. Multi-day windows are decomposed into daily openFDA API calls and landed as one raw batch. If the configured `max_pages` value is too low for a day, the client auto-extends after reading openFDA's reported total so the batch is not silently truncated.

Repeated raw runs for the same window create new immutable batches. Downstream curated/gold builds select the latest `ingest_batch_id` by default, or a specific batch can be supplied manually.

## Querying In Athena

After `openfda_refresh_metadata` succeeds, query the Glue database from Athena:

```sql
DESCRIBE pharma_cv_prod.curated_case_header;

SELECT *
FROM pharma_cv_prod.curated_case_header
LIMIT 10;
```

Example gold query:

```sql
SELECT report_year, report_month, case_count, serious_case_count
FROM pharma_cv_prod.gold_case_seriousness_trends
ORDER BY report_year DESC, report_month DESC
LIMIT 12;
```

Athena should use engine version 3 for native Delta Lake querying.

## Future Enhancement: Bedrock Summaries

A natural extension is to generate short narrative summaries from gold trend outputs using Amazon Bedrock. That capability would sit after the gold layer and should summarize observed reporting patterns only. It should not introduce diagnosis, causality claims, or medical recommendations.

Possible future outputs:
- monthly seriousness trend summary
- top co-reported drug-reaction movement summary
- demographic trend summary
- manufacturer or pharmacologic class monitoring summary

## Troubleshooting

If openFDA extraction fails, check the `extract_and_stage` task log. The client retries transient `429`, `500`, `502`, `503`, and `504` responses, treats openFDA `404 no matches found` as a successful no-data batch, and auto-extends low page limits when needed.

If `docker compose up` tries to download packages and fails with DNS errors, recreate the stack from the current compose file:

```bash
docker compose down
docker compose up -d postgres minio minio-create-bucket airflow-init airflow-scheduler airflow-webserver
```

If Athena cannot read Delta tables, confirm:
- the Databricks job ran with deletion vectors disabled
- existing incompatible curated/gold Delta folders were rebuilt after that config change
- Glue metadata was refreshed with `force_recreate=true`
- the Athena workgroup uses engine version 3

## Useful Commands

List service status:

```bash
docker compose ps
```

Follow scheduler logs:

```bash
docker compose logs -f airflow-scheduler
```

Stop the stack:

```bash
docker compose down
```
