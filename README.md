# Erudify - Senior Data Engineer Assignment (Option 1 Implementation)
This project implements a **local-first, Dockerized data pipeline** that answers the assignment in `manual.md` with:
- Open-source tools only (no cloud dependency)
- Airflow scheduling
- Great Expectations data quality validation
- FX-rate transformation from non-standard Excel format
- Loan outstanding conversion to USD in a mart-ready table

## 1) Stack
- **Orchestrator:** Apache Airflow 2.10
- **Warehouse:** PostgreSQL 16
- **ETL/Transform:** Python 3.11 + Pandas + SQLAlchemy
- **Validation:** Great Expectations
- **Runtime:** Docker Compose

## 2) Repository Layout
- `docker-compose.yml` - local stack definition
- `airflow/Dockerfile` - custom Airflow image with ETL + validation dependencies
- `airflow/dags/` - DAGs for FX monthly and Loanbook daily pipelines
- `pipeline/etl/` - source transformation code
- `pipeline/sql/` - DDL and mart SQL
- `pipeline/validations/` - Great Expectations checks
- `pipeline/cli.py` - local CLI commands
- `Makefile` - shortcut commands for setup, runs, validation, and stage queries
- `docs/` - assignment answers and thought process

## 3) Quick Start
### 3.1 Prepare environment file
```bash
cp .env.example .env
```

### 3.2 Start services
```bash
docker compose up --build airflow-init
docker compose up -d postgres airflow-webserver airflow-scheduler
```

### 3.3 Optional: use Makefile commands
```bash
make help
make init
make up
```

### 3.4 Access Airflow
- URL: `http://localhost:8080`
- Username: `admin` (default from `.env`)
- Password: `admin` (default from `.env`)

## 4) Airflow Schedules
- `fx_rate_monthly_pipeline`
  - Cron: `0 7 1-4 * *`
  - Why: Finance may upload month-end FX between day 1-4 of next month.
- `loanbook_daily_pipeline`
  - Cron: `0 4 * * *`
  - Why: Daily snapshot ingestion and mart refresh.

## 5) DAG Task Flow
Both DAGs follow quality-gated flow:
1. `init_db_objects`
2. ingest task (`ingest_fx_rates` or `ingest_loanbook`)
3. staging validation via Great Expectations
4. mart rebuild
5. mart validation via Great Expectations

If any validation fails, the task fails and the DAG run stops.

## 6) Running Pipelines from CLI (inside container)
### FX monthly flow
```bash
docker compose run --rm airflow-webserver python -m pipeline.cli run-fx-pipeline
```

### Loan daily flow
```bash
docker compose run --rm airflow-webserver python -m pipeline.cli run-loan-pipeline
```

### Individual commands
```bash
docker compose run --rm airflow-webserver python -m pipeline.cli init-db
docker compose run --rm airflow-webserver python -m pipeline.cli ingest-fx
docker compose run --rm airflow-webserver python -m pipeline.cli ingest-loanbook
docker compose run --rm airflow-webserver python -m pipeline.cli build-mart
docker compose run --rm airflow-webserver python -m pipeline.cli validate-fx
docker compose run --rm airflow-webserver python -m pipeline.cli validate-loanbook
docker compose run --rm airflow-webserver python -m pipeline.cli validate-mart
```

## 7) Data Model Summary
### Staging
- `staging.stg_fx_rate_long`
  - historical FX rates in long format (`rate_date`, `currency_code`, `rate_type`, `rate_to_usd`)
- `staging.stg_loanbook_snapshot`
  - daily snapshots from loanbook file

### Mart
- `mart.fct_loan_outstanding_usd`
  - joins loan snapshots with latest valid FX `closing_rate` (`fx.rate_date <= snapshot_date`)
  - computes `outstanding_balance_usd = outstanding_balance_local / rate_to_usd`
- `mart.dim_fx_rate_latest` (view)
  - latest FX per currency + rate type
## 8) Query Each Stage (What Happened and How to Read It)
### 8.1 FX Staging (`staging.stg_fx_rate_long`)
Run:
```bash
make query-fx-staging
```

What happened in this stage:
- raw wide FX Excel columns were unpivoted into long rows
- `rate_type` was assigned from section labels (`closing_rate`, `average_rate`)
- non-date headers (like `FY2024`) were ignored
- records were upserted by key (`rate_date`, `currency_code`, `rate_type`)

How to read output:
- each row should represent one currency and one rate type for one date
- `source_file` and `batch_id` show lineage and ingestion run identity

### 8.2 Loanbook Staging (`staging.stg_loanbook_snapshot`)
Run:
```bash
make query-loan-staging
```

What happened in this stage:
- snapshot rows were normalized from Excel
- date and numeric columns were parsed to typed values
- loan records were deduplicated by (`snapshot_date`, `loan_id`)
- local currency was assigned from `LOCAL_CURRENCY_CODE` (default `IDR`)

How to read output:
- one row = one loan snapshot at one date
- status should be in expected domain (`Submission`, `Activated`, `Closed`)
- `batch_id` identifies the load run

### 8.3 Mart Fact (`mart.fct_loan_outstanding_usd`)
Run:
```bash
make query-mart
```

What happened in this stage:
- each loan snapshot is joined to latest valid FX closing rate on or before snapshot date
- outstanding local amount is converted to USD by dividing by `fx_rate_to_usd`
- mart table is rebuilt each run for deterministic downstream consumption

How to read output:
- compare `outstanding_balance_local` vs `outstanding_balance_usd`
- verify `fx_rate_date <= snapshot_date`
- if `fx_rate_to_usd` is null, FX reference coverage is missing

### 8.4 Latest FX View (`mart.dim_fx_rate_latest`)
Run:
```bash
make query-latest-fx
```

What happened in this stage:
- view picks the newest FX row per (`currency_code`, `rate_type`)
- this is useful for quick operational checks and report defaults

### 8.5 Stage Audit Snapshot
Run:
```bash
make query-stage-audit
```

What happened in this stage:
- this query summarizes row counts and date ranges for each table
- use it to verify end-to-end freshness after DAG runs

## 9) Assumptions
1. `loanbook.xlsx` has no currency column, so local currency defaults to `IDR` (`LOCAL_CURRENCY_CODE` env var).
2. FX file values represent **local currency per 1 USD** (for example IDR around 14,000+), so conversion to USD uses division.
3. `fx_rate.xlsx` non-date marker cells (for example `FY2024`) are ignored during date parsing.
## 10) Assignment Answer Documents
- `docs/01_assignment_architecture.md`
- `docs/02_fx_data_model.md`
- `docs/03_pipeline_orchestration.md`
- `docs/04_data_quality_with_gx.md`
- `docs/05_thought_process_and_tooling.md`
## 11) Stop Services
```bash
docker compose down
```

To also remove volumes:
```bash
docker compose down -v
```
