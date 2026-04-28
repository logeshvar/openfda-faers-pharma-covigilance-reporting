# Pharma Covigilance Reporting Pipeline

Batch pharmacovigilance reporting pipeline for a Senior Data Engineer portfolio project.

This project ingests openFDA drug adverse event reports, preserves raw source fidelity, and incrementally builds curated and gold reporting layers on an AWS-style architecture.

Important boundary:
- This pipeline supports safety signal monitoring and reporting.
- It does not perform medical diagnosis.
- It does not prove causality between a drug and a reaction.

Current implementation status:
- Milestone 1 implemented
- Milestone 2 implemented
- Milestone 3 implemented
- Milestone 4 implemented
- Gold reporting layer implemented
- Glue/Athena metadata support implemented
- Raw ingestion path working end to end
- Curated transforms implemented for `curated_case_header`, `curated_primary_source`, and `curated_patient_demo`
- Curated/gold orchestration DAG prepares a Databricks Jobs API payload and can submit it when enabled

## Architecture

Target v1 architecture:
- Orchestration: Airflow
- Raw, ops, curated, and gold storage: S3-style layout
- Raw landing: immutable append-only JSON
- Processing: Databricks Spark jobs
- Metadata and query access: Glue Catalog and Athena

Local development choices for Milestone 1:
- Airflow runs in Docker Compose
- MinIO provides an S3-compatible storage target for local development only
- Postgres backs Airflow metadata

Architecture diagrams and the AWS transition plan live in [docs/architecture.md](</Users/logeshvar/Documents/Dubai/AWS Project/docs/architecture.md>).

## Repo Layout

Key folders:
- `airflow/dags/`: Airflow DAGs
- `conf/`: environment config
- `src/common/`: shared config and path helpers
- `src/ingestion/`: openFDA extraction and raw landing code
- `src/curated/`: curated layer transformation and orchestration helpers
- `src/dq/`: raw data quality checks
- `docs/`: architecture and design notes
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
- immutable raw batch layout where repeat runs create new batch objects instead of overwriting earlier landings

## Milestone 2 Progress

Implemented:
- `src/curated/build_case_header.py` Spark transformation for `curated_case_header`
- pure-Python normalization helpers and unit tests for the case header model
- curated storage config for `curated/` and `gold/`
- raw batch resolution logic for selecting the latest raw object for a requested window
- curated DQ checks for `curated_case_header`
- second DAG: `openfda_build_curated_gold` for Databricks curated job planning and optional submission

## Milestone 3 Progress

Implemented:
- `src/curated/build_primary_source.py` Spark transformation for `curated_primary_source`
- `src/curated/build_patient_demo.py` Spark transformation for `curated_patient_demo`
- Python normalization tests for qualification labels, sex labels, age unit labels, age conversion, age bands, and latest-row dedupe
- Databricks task generation for the three curated Spark scripts
- curated Databricks tasks are independent and can run in parallel from the same raw batch

## Milestone 4 Progress

Implemented:
- `src/curated/build_case_drug.py` for one row per drug within a report version
- `src/curated/build_case_drug_openfda.py` for openFDA drug arrays
- `src/curated/build_case_reaction.py` for one row per reaction within a report version

## Gold And Metadata Progress

Implemented:
- latest-case helper for gold reporting
- gold seriousness trend table
- gold drug-reaction co-reporting trend table
- gold reaction demographic trend table
- gold manufacturer/class seriousness trend table
- Glue/Athena registration helper and `openfda_refresh_metadata` DAG
- Athena validation SQL and basic reporting views
- gold Delta writes use `replaceWhere` for the affected `report_year`/`report_month` partitions instead of overwriting full history
- final gold trend tasks fan out in parallel after the latest-case helper is built

Deferred to later milestones:
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
docker compose exec airflow-scheduler airflow dags list | grep databricks_smoke_test
docker compose exec airflow-scheduler airflow dags list | grep openfda_build_curated_gold
docker compose exec airflow-scheduler airflow dags list | grep openfda_refresh_metadata
```

### 4. Trigger a manual backfill run

For smoke tests, prefer a one-day window. The extractor handles larger manual windows
by fetching openFDA one day at a time and combining the results into a single raw batch.
If `max_pages` is set lower than the page count needed for a day, the client logs a
warning and auto-extends to avoid landing an incomplete batch.

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_ingest_raw \
  --conf '{"window_start":"2025-03-01","window_end":"2025-03-01","page_size":100,"max_pages":50}'
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

Scheduled raw ingestion is source-lag aware. Because FAERS/openFDA updates are quarterly
and may lag by 3+ months, the default schedule runs daily but ingests a one-day window
120 days behind the Airflow logical date. This keeps API calls small, avoids partial
large-month extracts, and only processes data that is likely to be stable.

`databricks_smoke_test` submits a single Git-backed Databricks Python task that:
1. creates a Databricks job cluster
2. uses the configured AWS instance profile
3. runs `src/databricks/smoke_test_s3_access.py` from Git source
4. lists the configured S3 bucket/prefix
5. writes a marker file under `s3://<bucket>/ops/smoke_tests/`

`openfda_build_curated_gold` currently does the following:
1. resolves the requested reporting window
2. lists raw NDJSON objects for that window from S3-compatible storage
3. selects the latest `ingest_batch_id` unless one is explicitly supplied
4. prepares job manifests for all curated tables
5. builds Git-backed Databricks saved-job settings with a shared job cluster
6. includes dependency-aware gold table tasks in the same job
7. creates or resets the saved Databricks job and triggers `run-now` when `DATABRICKS_SUBMIT_ENABLED=true`; otherwise it logs a dry-run result

For manual curated/gold builds, use the same `window_start` / `window_end` that already
exists in raw storage. If you just ingested a one-day smoke-test window, trigger curated/gold
with that same one-day window.

`openfda_refresh_metadata` currently does the following:
1. prepares Athena DDL for curated and gold Delta table locations
2. creates the Glue database and starts Athena DDL queries only when triggered with `{"execute_refresh": true}`
3. returns the Athena query execution IDs for follow-up monitoring

Manual backfill parameters:

```json
{
  "window_start": "2025-03-01",
  "window_end": "2025-03-01",
  "page_size": 100,
  "max_pages": 200
}
```

## Databricks Git Jobs

Production config uses Databricks Git source execution instead of S3-staged Python files.
The curated/gold DAG uses a saved Databricks Job so all curated and gold tasks can share
one jobs cluster. The smoke-test DAG still uses one-time `runs/submit`, which is appropriate
for a single task. The Databricks job settings include one top-level `git_source` block and
task-level relative Python paths such as `src/curated/build_case_header.py`.

Required prod values:

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

The AWS Secrets Manager secret should contain only Databricks API auth:

```json
{
  "host": "https://dbc-xxxx.cloud.databricks.com",
  "token": "your-databricks-token"
}
```

Run the Databricks smoke test as a dry run:

```bash
docker compose exec airflow-scheduler airflow dags trigger databricks_smoke_test \
  --conf '{"dry_run": true}'
```

Submit the Databricks smoke test:

```bash
docker compose exec airflow-scheduler airflow dags trigger databricks_smoke_test \
  --conf '{"dry_run": false}'
```

Build curated/gold payload without submitting:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_build_curated_gold \
  --conf '{"window_start":"2025-03-01","window_end":"2025-03-01","dry_run":true}'
```

Submit curated/gold to Databricks:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_build_curated_gold \
  --conf '{"window_start":"2025-03-01","window_end":"2025-03-01","dry_run":false}'
```

Refresh Glue/Athena metadata. Use `force_recreate` when existing Glue Delta table entries were created with stale or empty schema metadata:

```bash
docker compose exec airflow-scheduler airflow dags trigger openfda_refresh_metadata \
  --conf '{"execute_refresh":true,"force_recreate":true}'
```

## Local S3 Prefixes

Raw data:
- `raw/openfda/drug_event/query_year=YYYY/query_month=MM/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/ingest_batch_id=.../*.ndjson`

Audit data:
- `ops/audit/source_name=openfda_drug_event/query_year=YYYY/query_month=MM/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/*.json`

Curated data target:
- `curated/curated_case_header`
- `curated/curated_primary_source`
- `curated/curated_patient_demo`
- `curated/curated_case_drug`
- `curated/curated_case_drug_openfda`
- `curated/curated_case_reaction`

Gold data target:
- `gold/gold_latest_case_helper`
- `gold/gold_case_seriousness_trends`
- `gold/gold_drug_reaction_trends`
- `gold/gold_reaction_demographic_trends`
- `gold/gold_manufacturer_class_serious_trends`

## Target AWS Configuration

For target-style execution, use `conf/prod.yaml` as the shape of the AWS config:
- `S3_ENDPOINT_URL` should be unset so boto3 uses Amazon S3.
- `S3_BUCKET` should point to the real project bucket.
- Airflow should use an IAM role with S3 read/write access instead of static local keys.
- `DATABRICKS_HOST` and `DATABRICKS_TOKEN` should come from Secrets Manager, an Airflow connection, or the managed runtime environment.
- Databricks execution uses Git source by default, so `DATABRICKS_GIT_URL`, `DATABRICKS_GIT_PROVIDER`, and `DATABRICKS_GIT_BRANCH` identify the repo revision to run.
- `DATABRICKS_PYTHON_FILE_BASE_URI` is only needed for the legacy S3 script-source mode.
- Databricks job clusters disable Delta deletion vectors by default so S3 Delta tables remain compatible with Athena's native Delta reader.

## Current Assumptions

- Scheduled runs ingest a lagged one-day window by default because FAERS/openFDA updates quarterly and can lag by 3+ months.
- The extraction window is based on openFDA `receivedate`.
- The local S3 bucket is created automatically during Docker startup.
- Raw DQ failures still write an audit record before the DAG fails.
- Manual multi-day backfills are decomposed into daily openFDA API queries and landed as one raw batch.
- If a configured `max_pages` value is too low for a day, the client auto-extends after reading openFDA's reported total.
- Curated tables merge by their report-version business keys once a Delta table exists.
- Gold tables rebuild only the affected `report_year`/`report_month` partitions for the run window.

## Troubleshooting

If the extractor fails before landing raw data, check the `extract_and_stage` task log for the
openFDA HTTP status and response body. The client retries transient `429`, `500`, `502`, `503`,
and `504` responses, treats openFDA `404 no matches found` as a successful `NO_DATA` batch, and
auto-extends low `max_pages` values to avoid incomplete raw batches.

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
