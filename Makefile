SHELL := /bin/bash

WINDOW_START ?= 2025-12-28
WINDOW_END ?= $(WINDOW_START)
PAGE_SIZE ?= 100
MAX_PAGES ?= 200
DRY_RUN ?= false
FORCE_RECREATE ?= true
MAX_DEMO_WINDOW_DAYS ?= 7

AIRFLOW_SERVICE ?= airflow-scheduler
AIRFLOW := docker compose exec $(AIRFLOW_SERVICE) airflow
PYTHON := docker compose exec $(AIRFLOW_SERVICE) python
RUN_TAG ?= $(shell date -u +%Y%m%dT%H%M%SZ)

RAW_RUN_ID ?= pharma_cv_raw_$(RUN_TAG)
SMOKE_RUN_ID ?= pharma_cv_smoke_$(RUN_TAG)
CURATED_GOLD_RUN_ID ?= pharma_cv_curated_gold_$(RUN_TAG)
METADATA_RUN_ID ?= pharma_cv_metadata_$(RUN_TAG)

RAW_CONF := {"window_start":"$(WINDOW_START)","window_end":"$(WINDOW_END)","page_size":$(PAGE_SIZE),"max_pages":$(MAX_PAGES)}
SMOKE_CONF := {"dry_run":$(DRY_RUN)}
CURATED_GOLD_CONF := {"window_start":"$(WINDOW_START)","window_end":"$(WINDOW_END)","dry_run":$(DRY_RUN)}
METADATA_CONF := {"execute_refresh":true,"force_recreate":$(FORCE_RECREATE)}

.PHONY: help up down ps logs dags raw-ingest raw-ingest-wait databricks-smoke databricks-smoke-wait curated-gold-dry-run curated-gold curated-gold-wait metadata-refresh metadata-refresh-wait demo-run demo-run-no-smoke

help:
	@echo "Pharma Covigilance pipeline commands"
	@echo ""
	@echo "Common targets:"
	@echo "  make up                         Start local Airflow/MinIO stack"
	@echo "  make dags                       List project DAGs"
	@echo "  make raw-ingest                 Trigger openFDA raw ingest"
	@echo "  make raw-ingest-wait            Trigger raw ingest and wait for Airflow completion"
	@echo "  make databricks-smoke           Submit Databricks S3 smoke test"
	@echo "  make databricks-smoke-wait      Submit smoke test and wait for Databricks completion"
	@echo "  make curated-gold-dry-run       Build curated/gold Databricks payload without submitting"
	@echo "  make curated-gold               Submit curated/gold Databricks job"
	@echo "  make curated-gold-wait          Submit curated/gold and wait for Databricks completion"
	@echo "  make metadata-refresh           Recreate Glue metadata and wait for Athena DDL"
	@echo "  make demo-run                   Run full ordered flow with waits"
	@echo ""
	@echo "Useful variables:"
	@echo "  WINDOW_START=YYYY-MM-DD         Default: $(WINDOW_START)"
	@echo "  WINDOW_END=YYYY-MM-DD           Default: $(WINDOW_END)"
	@echo "  PAGE_SIZE=100                   Default: $(PAGE_SIZE)"
	@echo "  MAX_PAGES=200                   Default: $(MAX_PAGES)"
	@echo "  DRY_RUN=false                   Default: $(DRY_RUN)"
	@echo "  FORCE_RECREATE=true             Default: $(FORCE_RECREATE)"
	@echo "  MAX_DEMO_WINDOW_DAYS=7          Default: $(MAX_DEMO_WINDOW_DAYS)"
	@echo ""
	@echo "Example:"
	@echo "  make demo-run WINDOW_START=2025-10-01 WINDOW_END=2025-10-07 MAX_PAGES=500"
	@echo "  make demo-run-no-smoke WINDOW_START=2025-10-08 WINDOW_END=2025-10-14 MAX_PAGES=500"

up:
	docker compose up -d postgres minio minio-create-bucket airflow-init airflow-scheduler airflow-webserver

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f airflow-scheduler

dags:
	$(AIRFLOW) dags list | grep -E 'openfda_ingest_raw|databricks_smoke_test|openfda_build_curated_gold|openfda_refresh_metadata'

raw-ingest:
	$(AIRFLOW) dags trigger openfda_ingest_raw --conf '$(RAW_CONF)'

raw-ingest-wait:
	$(AIRFLOW) dags trigger openfda_ingest_raw --run-id '$(RAW_RUN_ID)' --conf '$(RAW_CONF)'
	$(PYTHON) /opt/airflow/config/wait_for_dag_run.py openfda_ingest_raw '$(RAW_RUN_ID)'

databricks-smoke:
	$(AIRFLOW) dags trigger databricks_smoke_test --conf '$(SMOKE_CONF)'

databricks-smoke-wait:
	$(AIRFLOW) dags trigger databricks_smoke_test --run-id '$(SMOKE_RUN_ID)' --conf '{"dry_run":false}'
	$(PYTHON) /opt/airflow/config/wait_for_dag_run.py databricks_smoke_test '$(SMOKE_RUN_ID)'
	$(PYTHON) /opt/airflow/config/wait_for_databricks_run.py --airflow-dag-id databricks_smoke_test --airflow-run-id '$(SMOKE_RUN_ID)' --task-id submit_or_log

curated-gold-dry-run:
	$(AIRFLOW) dags trigger openfda_build_curated_gold --conf '{"window_start":"$(WINDOW_START)","window_end":"$(WINDOW_END)","dry_run":true}'

curated-gold:
	$(AIRFLOW) dags trigger openfda_build_curated_gold --conf '$(CURATED_GOLD_CONF)'

curated-gold-wait:
	$(AIRFLOW) dags trigger openfda_build_curated_gold --run-id '$(CURATED_GOLD_RUN_ID)' --conf '{"window_start":"$(WINDOW_START)","window_end":"$(WINDOW_END)","dry_run":false}'
	$(PYTHON) /opt/airflow/config/wait_for_dag_run.py openfda_build_curated_gold '$(CURATED_GOLD_RUN_ID)'
	$(PYTHON) /opt/airflow/config/wait_for_databricks_run.py --airflow-dag-id openfda_build_curated_gold --airflow-run-id '$(CURATED_GOLD_RUN_ID)' --task-id submit_or_log_databricks_run

metadata-refresh:
	$(AIRFLOW) dags trigger openfda_refresh_metadata --conf '$(METADATA_CONF)'

metadata-refresh-wait:
	$(AIRFLOW) dags trigger openfda_refresh_metadata --run-id '$(METADATA_RUN_ID)' --conf '$(METADATA_CONF)'
	$(PYTHON) /opt/airflow/config/wait_for_dag_run.py openfda_refresh_metadata '$(METADATA_RUN_ID)'

demo-run:
	$(PYTHON) /opt/airflow/config/validate_window_days.py --window-start '$(WINDOW_START)' --window-end '$(WINDOW_END)' --max-days '$(MAX_DEMO_WINDOW_DAYS)'
	$(MAKE) raw-ingest-wait RUN_TAG='$(RUN_TAG)' WINDOW_START='$(WINDOW_START)' WINDOW_END='$(WINDOW_END)' PAGE_SIZE='$(PAGE_SIZE)' MAX_PAGES='$(MAX_PAGES)'
	$(MAKE) databricks-smoke-wait RUN_TAG='$(RUN_TAG)'
	$(MAKE) curated-gold-wait RUN_TAG='$(RUN_TAG)' WINDOW_START='$(WINDOW_START)' WINDOW_END='$(WINDOW_END)'
	$(MAKE) metadata-refresh-wait RUN_TAG='$(RUN_TAG)' FORCE_RECREATE='$(FORCE_RECREATE)'

demo-run-no-smoke:
	$(PYTHON) /opt/airflow/config/validate_window_days.py --window-start '$(WINDOW_START)' --window-end '$(WINDOW_END)' --max-days '$(MAX_DEMO_WINDOW_DAYS)'
	$(MAKE) raw-ingest-wait RUN_TAG='$(RUN_TAG)' WINDOW_START='$(WINDOW_START)' WINDOW_END='$(WINDOW_END)' PAGE_SIZE='$(PAGE_SIZE)' MAX_PAGES='$(MAX_PAGES)'
	$(MAKE) curated-gold-wait RUN_TAG='$(RUN_TAG)' WINDOW_START='$(WINDOW_START)' WINDOW_END='$(WINDOW_END)'
	$(MAKE) metadata-refresh-wait RUN_TAG='$(RUN_TAG)' FORCE_RECREATE='$(FORCE_RECREATE)'
