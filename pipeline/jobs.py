from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import pandas as pd

from sqlalchemy import text

from pipeline.config import settings
from pipeline.db import get_engine, query_dataframe, run_sql_file
from pipeline.etl.fx_rate import transform_fx_rate_file
from pipeline.etl.loanbook import transform_loanbook_file
from pipeline.validations.ge_validations import (
    ValidationSummary,
    validate_fx_staging_dataframe,
    validate_loan_staging_dataframe,
    validate_mart_dataframe,
)

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parent / "sql"


def _make_batch_id(prefix: str) -> str:
    now_utc = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{now_utc}_{uuid4().hex[:8]}"


def _raise_if_validation_failed(summary: ValidationSummary) -> None:
    if summary.success:
        logger.info("Validation passed: %s", summary.to_dict())
        return
    raise RuntimeError(f"Validation failed: {summary.to_dict()}")


def ensure_database_objects() -> None:
    logger.info("Ensuring schemas and tables exist")
    engine = get_engine()
    run_sql_file(engine, SQL_DIR / "create_schemas.sql")
    run_sql_file(engine, SQL_DIR / "create_tables.sql")


def run_fx_ingestion(file_path: str | None = None) -> int:
    source_path = Path(file_path or settings.fx_rate_filepath)
    logger.info("Starting FX ingestion from %s", source_path)

    fx_rates = transform_fx_rate_file(source_path)
    ingested_at = datetime.now(timezone.utc)
    batch_id = _make_batch_id("fx")

    fx_rates["source_file"] = source_path.name
    fx_rates["ingested_at"] = ingested_at
    fx_rates["batch_id"] = batch_id
    fx_rates = fx_rates.where(pd.notna(fx_rates), None)
    records = fx_rates.to_dict(orient="records")
    if not records:
        raise RuntimeError(f"No FX records to ingest from {source_path}")

    upsert_sql = text(
        """
        INSERT INTO staging.stg_fx_rate_long (
            rate_date, currency_code, rate_type, rate_to_usd, source_file, ingested_at, batch_id
        ) VALUES (
            :rate_date, :currency_code, :rate_type, :rate_to_usd, :source_file, :ingested_at, :batch_id
        )
        ON CONFLICT (rate_date, currency_code, rate_type) DO UPDATE
        SET
            rate_to_usd = EXCLUDED.rate_to_usd,
            source_file = EXCLUDED.source_file,
            ingested_at = EXCLUDED.ingested_at,
            batch_id = EXCLUDED.batch_id
        """
    )
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(upsert_sql, records)

    logger.info("FX ingestion completed: %s rows upserted", len(records))
    return len(records)


def run_loanbook_ingestion(file_path: str | None = None) -> int:
    source_path = Path(file_path or settings.loanbook_filepath)
    logger.info("Starting loanbook ingestion from %s", source_path)

    loanbook = transform_loanbook_file(
        source_path, currency_code=settings.local_currency_code
    )
    ingested_at = datetime.now(timezone.utc)
    batch_id = _make_batch_id("loanbook")

    loanbook["source_file"] = source_path.name
    loanbook["ingested_at"] = ingested_at
    loanbook["batch_id"] = batch_id
    loanbook = loanbook.where(pd.notna(loanbook), None)
    records = loanbook.to_dict(orient="records")
    if not records:
        raise RuntimeError(f"No loanbook records to ingest from {source_path}")

    upsert_sql = text(
        """
        INSERT INTO staging.stg_loanbook_snapshot (
            snapshot_date, loan_id, requested_principal, outstanding_balance, status,
            currency_code, source_file, ingested_at, batch_id
        ) VALUES (
            :snapshot_date, :loan_id, :requested_principal, :outstanding_balance, :status,
            :currency_code, :source_file, :ingested_at, :batch_id
        )
        ON CONFLICT (snapshot_date, loan_id) DO UPDATE
        SET
            requested_principal = EXCLUDED.requested_principal,
            outstanding_balance = EXCLUDED.outstanding_balance,
            status = EXCLUDED.status,
            currency_code = EXCLUDED.currency_code,
            source_file = EXCLUDED.source_file,
            ingested_at = EXCLUDED.ingested_at,
            batch_id = EXCLUDED.batch_id
        """
    )
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(upsert_sql, records)

    logger.info("Loanbook ingestion completed: %s rows upserted", len(records))
    return len(records)


def build_loan_outstanding_mart() -> None:
    logger.info("Building mart.fct_loan_outstanding_usd")
    engine = get_engine()
    run_sql_file(engine, SQL_DIR / "build_mart_loan_outstanding_usd.sql")


def validate_fx_staging() -> ValidationSummary:
    engine = get_engine()
    dataframe = query_dataframe(engine, "SELECT * FROM staging.stg_fx_rate_long")
    summary = validate_fx_staging_dataframe(dataframe)
    _raise_if_validation_failed(summary)
    return summary


def validate_loan_staging() -> ValidationSummary:
    engine = get_engine()
    dataframe = query_dataframe(engine, "SELECT * FROM staging.stg_loanbook_snapshot")
    summary = validate_loan_staging_dataframe(dataframe)
    _raise_if_validation_failed(summary)
    return summary


def validate_mart() -> ValidationSummary:
    engine = get_engine()
    dataframe = query_dataframe(engine, "SELECT * FROM mart.fct_loan_outstanding_usd")
    summary = validate_mart_dataframe(dataframe)
    _raise_if_validation_failed(summary)
    return summary
