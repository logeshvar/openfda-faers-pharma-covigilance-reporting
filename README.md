# Pharma Pharmacovigilance Reporting Pipeline

Batch pharmacovigilance reporting pipeline for a Senior Data Engineer portfolio project.

This project ingests openFDA drug adverse event reports, preserves raw source fidelity, and incrementally builds curated and gold reporting layers on an AWS-style architecture.

Important boundary:
- This pipeline supports safety signal monitoring and reporting.
- It does not perform medical diagnosis.
- It does not prove causality between a drug and a reaction.

Current implementation status:
- Milestone 1 in progress
- Raw ingestion path scaffolded
- Airflow DAG for raw batch ingestion implemented
- Curated and gold transformations not implemented yet

## Architecture

Target v1 architecture:
- Orchestration: Airflow
- Raw, ops, curated, and gold storage: S3-style layout
- Raw landing: immutable append-only JSON
- Processing: Databricks Spark jobs in later milestones
- Metadata and query access: Glue Catalog and Athena in later milestones

Local development choices for Milestone 1:
- Airflow runs in Docker Compose
- MinIO provides an S3-compatible storage target for local development
- Postgres backs Airflow metadata

## Repo Layout

Key folders:
- `airflow/dags/`: Airflow DAGs
- `conf/`: environment config
- `src/common/`: shared config and path helpers
- `src/ingestion/`: openFDA extraction and raw landing code
- `src/dq/`: raw data quality checks
- `tests/`: reserved for unit and integration tests

## Milestone 1 Scope

Implemented:
- local Airflow + MinIO bootstrap
- centralized config loader
- openFDA API client with retry handling
- window-based extraction and local staging
- raw NDJSON landing to S3-compatible storage
- ingest audit record writer
- raw DQ checks
- first TaskFlow DAG: `openfda_ingest_raw`

Deferred to later milestones:
- curated Delta tables
- Databricks Spark transforms
- Glue Catalog and Athena refresh
- gold reporting datasets
- Bedrock summaries

## Quick Start

### 1. Create local environment file

```bash
cp .env.example .env
```

You can keep the default values for local development.

### 2. Start the local stack

```bash
docker compose up -d postgres minio minio-create-bucket airflow-init airflow-scheduler airflow-webserver
```

The compose startup is intentionally offline-safe: it does not `pip install` from PyPI during container boot.

If you previously tried an older version of this compose file and hit package download errors, recreate the stack first:

```bash
docker compose down
docker compose up -d postgres minio minio-create-bucket airflow-init airflow-scheduler airflow-webserver
```

Airflow UI:
- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

If port `8080` is already in use locally, change `AIRFLOW_WEBSERVER_PORT` in `.env` and use that port in the URL instead.

MinIO:
- API: `http://localhost:9000`
- Console: `http://localhost:9001`
- Username: `minioadmin`
- Password: `minioadmin`

### 3. Confirm the DAG is present

```bash
docker compose exec airflow-scheduler airflow dags list | grep openfda_ingest_raw
```

### 4. Trigger a manual backfill run

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_ingest_raw \
  --conf '{"window_start":"2026-03-01","window_end":"2026-03-31","page_size":100,"max_pages":50}'
```

### 5. Inspect logs

```bash
docker compose logs -f airflow-scheduler
```

## How The DAG Works

`openfda_ingest_raw` does the following:
1. resolves the batch window from the Airflow schedule or manual `dag_run.conf`
2. extracts openFDA records for the requested window
3. stages the extracted payload locally inside the Airflow container
4. runs raw DQ checks
5. writes immutable raw NDJSON to the raw S3 prefix when DQ passes
6. writes an ingest audit record to the ops S3 prefix

Manual backfill parameters:

```json
{
  "window_start": "2026-03-01",
  "window_end": "2026-03-31",
  "page_size": 100,
  "max_pages": 200
}
```

## Local S3 Prefixes

Raw data:
- `raw/openfda/drug_event/query_year=YYYY/query_month=MM/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/ingest_batch_id=.../*.ndjson`

Audit data:
- `ops/audit/source_name=openfda_drug_event/query_year=YYYY/query_month=MM/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/*.json`

## Current Assumptions

- Monthly runs use the Airflow data interval and ingest the full prior interval.
- The extraction window is based on openFDA `receivedate`.
- The local S3 bucket is created automatically during Docker startup.
- Raw DQ failures still write an audit record before the DAG fails.
- If openFDA pagination would truncate the batch, the client raises an error instead of silently landing partial data.

## Troubleshooting

If the extractor fails with a message like `openFDA extraction stopped before all records were retrieved`, either:
- increase `max_pages` for that backfill run
- increase `page_size` up to the openFDA limit of `100`
- narrow the requested date window

If `docker compose up` fails with a `Temporary failure in name resolution` error while trying to install packages like `PyYAML`, you are likely running an older compose definition that still installs Python packages during container startup. Pull the latest `docker-compose.yml`, run `docker compose down`, and start the stack again. The current repo version does not install packages from PyPI during `up`.

## Useful Commands

List service status:

```bash
docker compose ps
```

Stop the stack:

```bash
docker compose down
```

Stop the stack and remove volumes:

```bash
docker compose down -v
```
