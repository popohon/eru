SHELL := /bin/bash

COMPOSE := docker compose
AIRFLOW_SERVICE := airflow-webserver
POSTGRES_SERVICE := postgres
POSTGRES_USER := airflow
WAREHOUSE_DB := warehouse

.PHONY: help env init up down restart logs ps airflow-init run-fx run-loan validate-all psql query-fx-staging query-loan-staging query-mart query-latest-fx query-stage-audit ppt-sync

help:
	@printf "\nTargets:\n"
	@printf "  make env               # create .env from .env.example if missing\n"
	@printf "  make init              # run airflow-init only (db migrate + airflow user)\n"
	@printf "  make up                # start postgres + airflow webserver + scheduler\n"
	@printf "  make down              # stop all services\n"
	@printf "  make restart           # restart all services\n"
	@printf "  make logs              # stream all service logs\n"
	@printf "  make ps                # list compose service status\n"
	@printf "  make run-fx            # run full FX pipeline via CLI in airflow container\n"
	@printf "  make run-loan          # run full Loan pipeline via CLI in airflow container\n"
	@printf "  make validate-all      # run FX, Loanbook, and Mart validations\n"
	@printf "  make psql              # open psql shell to warehouse DB inside postgres container\n"
	@printf "  make query-fx-staging  # inspect FX staging table\n"
	@printf "  make query-loan-staging# inspect loanbook staging table\n"
	@printf "  make query-mart        # inspect USD mart output table\n"
	@printf "  make query-latest-fx   # inspect latest FX view\n"
	@printf "  make query-stage-audit # stage-level row count + date range sanity checks\n\n"
	@printf "  make ppt-sync          # regenerate ppt.pptx as 1:1 copy from README/docs\n\n"

env:
	@if [[ ! -f .env ]]; then cp .env.example .env; echo ".env created from .env.example"; else echo ".env already exists"; fi

airflow-init: env
	$(COMPOSE) up --build airflow-init

init: airflow-init

up: env
	$(COMPOSE) up -d postgres airflow-webserver airflow-scheduler

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

run-fx:
	$(COMPOSE) run --rm $(AIRFLOW_SERVICE) python -m pipeline.cli run-fx-pipeline

run-loan:
	$(COMPOSE) run --rm $(AIRFLOW_SERVICE) python -m pipeline.cli run-loan-pipeline

validate-all:
	$(COMPOSE) run --rm $(AIRFLOW_SERVICE) python -m pipeline.cli validate-fx
	$(COMPOSE) run --rm $(AIRFLOW_SERVICE) python -m pipeline.cli validate-loanbook
	$(COMPOSE) run --rm $(AIRFLOW_SERVICE) python -m pipeline.cli validate-mart

psql:
	$(COMPOSE) exec -it $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(WAREHOUSE_DB)

query-fx-staging:
	$(COMPOSE) exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(WAREHOUSE_DB) -c "SELECT rate_date, currency_code, rate_type, rate_to_usd, source_file, batch_id FROM staging.stg_fx_rate_long ORDER BY rate_date DESC, currency_code, rate_type LIMIT 20;"

query-loan-staging:
	$(COMPOSE) exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(WAREHOUSE_DB) -c "SELECT snapshot_date, loan_id, requested_principal, outstanding_balance, status, currency_code, batch_id FROM staging.stg_loanbook_snapshot ORDER BY snapshot_date DESC, loan_id LIMIT 20;"

query-mart:
	$(COMPOSE) exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(WAREHOUSE_DB) -c "SELECT snapshot_date, loan_id, currency_code, outstanding_balance_local, fx_rate_to_usd, fx_rate_date, outstanding_balance_usd FROM mart.fct_loan_outstanding_usd ORDER BY snapshot_date DESC, loan_id LIMIT 20;"

query-latest-fx:
	$(COMPOSE) exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(WAREHOUSE_DB) -c "SELECT currency_code, rate_type, rate_date, rate_to_usd, source_file FROM mart.dim_fx_rate_latest ORDER BY currency_code, rate_type;"

query-stage-audit:
	$(COMPOSE) exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(WAREHOUSE_DB) -c "SELECT 'staging.stg_fx_rate_long' AS table_name, COUNT(*) AS row_count, MIN(rate_date) AS min_date, MAX(rate_date) AS max_date FROM staging.stg_fx_rate_long UNION ALL SELECT 'staging.stg_loanbook_snapshot' AS table_name, COUNT(*) AS row_count, MIN(snapshot_date) AS min_date, MAX(snapshot_date) AS max_date FROM staging.stg_loanbook_snapshot UNION ALL SELECT 'mart.fct_loan_outstanding_usd' AS table_name, COUNT(*) AS row_count, MIN(snapshot_date) AS min_date, MAX(snapshot_date) AS max_date FROM mart.fct_loan_outstanding_usd;"

ppt-sync:
	.venv/bin/python scripts/generate_ppt_from_docs.py
