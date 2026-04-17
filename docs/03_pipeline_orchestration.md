# Pipeline Design and Orchestration
## Pipeline Components
1. **Ingestion/Transform code** (`pipeline/etl/`)
2. **Warehouse load + mart build** (`pipeline/jobs.py`, `pipeline/sql/`)
3. **Quality gates** (Great Expectations in `pipeline/validations/`)
4. **Scheduling + retries** (Airflow DAGs in `airflow/dags/`)

## DAG 1 - `fx_rate_monthly_pipeline`
Schedule:
- `0 7 1-4 * *`

Rationale:
- Finance updates month-end file, but arrival may vary between day 1-4 of following month.
- This schedule captures delayed submissions without manual intervention.

Task order:
1. `init_db_objects`
2. `ingest_fx_rates`
3. `validate_fx_staging`
4. `rebuild_loan_outstanding_mart`
5. `validate_mart_after_fx_refresh`

## DAG 2 - `loanbook_daily_pipeline`
Schedule:
- `0 4 * * *`

Rationale:
- Daily snapshot pipeline to support BAU monitoring and downstream reporting refresh.

Task order:
1. `init_db_objects`
2. `ingest_loanbook`
3. `validate_loanbook_staging`
4. `rebuild_loan_outstanding_mart`
5. `validate_mart_after_loan_refresh`

## Reliability Controls
- Retries: 2 attempts
- Retry delay: 5 minutes
- Task execution timeouts to prevent hanging tasks
- DAG-level `max_active_runs=1` to avoid overlapping runs

## Idempotency Strategy
- Upserts with `ON CONFLICT` in staging tables
- Deterministic primary keys on business keys
- Mart table rebuilt from latest staging snapshot using truncate + insert

## Operational Notes
- If validation fails, Airflow task fails and pipeline stops.
- This protects regulatory and management reporting from bad or incomplete data.
- Re-runs are safe due to idempotent staging write behavior.

## Stage Query Playbook (How to Inspect What Happened)
## Stage A - FX staging load result
Query:
```sql
SELECT rate_date, currency_code, rate_type, rate_to_usd, source_file, batch_id
FROM staging.stg_fx_rate_long
ORDER BY rate_date DESC, currency_code, rate_type
LIMIT 20;
```

Interpretation:
- confirms unpivot from wide excel layout into long history rows
- verifies `rate_type` assignment and lineage columns from latest run

## Stage B - Loanbook staging load result
Query:
```sql
SELECT snapshot_date, loan_id, requested_principal, outstanding_balance, status, currency_code, batch_id
FROM staging.stg_loanbook_snapshot
ORDER BY snapshot_date DESC, loan_id
LIMIT 20;
```

Interpretation:
- confirms typed parsing and deduplicated snapshot keys
- verifies domain normalization (`status`) and assigned local currency

## Stage C - Mart transformation result
Query:
```sql
SELECT snapshot_date, loan_id, currency_code, outstanding_balance_local, fx_rate_to_usd, fx_rate_date, outstanding_balance_usd
FROM mart.fct_loan_outstanding_usd
ORDER BY snapshot_date DESC, loan_id
LIMIT 20;
```

Interpretation:
- confirms as-of FX join behavior (`fx_rate_date <= snapshot_date`)
- confirms local-to-USD translation logic

## Stage D - Current FX operational view
Query:
```sql
SELECT currency_code, rate_type, rate_date, rate_to_usd, source_file
FROM mart.dim_fx_rate_latest
ORDER BY currency_code, rate_type;
```

Interpretation:
- confirms latest available rate per currency and rate type
- useful for report defaults and operational checks

## Stage E - End-to-end stage audit
Query:
```sql
SELECT 'staging.stg_fx_rate_long' AS table_name, COUNT(*) AS row_count, MIN(rate_date) AS min_date, MAX(rate_date) AS max_date
FROM staging.stg_fx_rate_long
UNION ALL
SELECT 'staging.stg_loanbook_snapshot' AS table_name, COUNT(*) AS row_count, MIN(snapshot_date) AS min_date, MAX(snapshot_date) AS max_date
FROM staging.stg_loanbook_snapshot
UNION ALL
SELECT 'mart.fct_loan_outstanding_usd' AS table_name, COUNT(*) AS row_count, MIN(snapshot_date) AS min_date, MAX(snapshot_date) AS max_date
FROM mart.fct_loan_outstanding_usd;
```

Interpretation:
- validates row availability at each stage
- validates date freshness after each DAG run
