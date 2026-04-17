from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import jobs

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="loanbook_daily_pipeline",
    default_args=default_args,
    description="Daily loanbook ingestion and USD mart refresh",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["erudify", "loanbook", "assignment"],
) as dag:
    init_db = PythonOperator(
        task_id="init_db_objects",
        python_callable=jobs.ensure_database_objects,
        execution_timeout=timedelta(minutes=5),
    )

    ingest_loanbook = PythonOperator(
        task_id="ingest_loanbook",
        python_callable=jobs.run_loanbook_ingestion,
        execution_timeout=timedelta(minutes=15),
    )

    validate_loanbook = PythonOperator(
        task_id="validate_loanbook_staging",
        python_callable=jobs.validate_loan_staging,
        execution_timeout=timedelta(minutes=10),
    )

    rebuild_mart = PythonOperator(
        task_id="rebuild_loan_outstanding_mart",
        python_callable=jobs.build_loan_outstanding_mart,
        execution_timeout=timedelta(minutes=10),
    )

    validate_mart = PythonOperator(
        task_id="validate_mart_after_loan_refresh",
        python_callable=jobs.validate_mart,
        execution_timeout=timedelta(minutes=10),
    )

    init_db >> ingest_loanbook >> validate_loanbook >> rebuild_mart >> validate_mart
