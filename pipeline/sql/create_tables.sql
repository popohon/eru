CREATE TABLE IF NOT EXISTS staging.stg_fx_rate_long (
    rate_date DATE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    rate_type VARCHAR(32) NOT NULL,
    rate_to_usd NUMERIC(20, 8) NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    batch_id TEXT NOT NULL,
    PRIMARY KEY (rate_date, currency_code, rate_type)
);

CREATE TABLE IF NOT EXISTS staging.stg_loanbook_snapshot (
    snapshot_date DATE NOT NULL,
    loan_id TEXT NOT NULL,
    requested_principal NUMERIC(20, 2),
    outstanding_balance NUMERIC(20, 2),
    status TEXT,
    currency_code VARCHAR(3) NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    batch_id TEXT NOT NULL,
    PRIMARY KEY (snapshot_date, loan_id)
);

CREATE TABLE IF NOT EXISTS mart.fct_loan_outstanding_usd (
    snapshot_date DATE NOT NULL,
    loan_id TEXT NOT NULL,
    status TEXT,
    currency_code VARCHAR(3) NOT NULL,
    outstanding_balance_local NUMERIC(20, 2) NOT NULL,
    fx_rate_to_usd NUMERIC(20, 8),
    fx_rate_date DATE,
    outstanding_balance_usd NUMERIC(20, 2),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, loan_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_fx_currency_date
    ON staging.stg_fx_rate_long (currency_code, rate_type, rate_date DESC);

CREATE OR REPLACE VIEW mart.dim_fx_rate_latest AS
SELECT DISTINCT ON (currency_code, rate_type)
    currency_code,
    rate_type,
    rate_date,
    rate_to_usd,
    source_file,
    ingested_at,
    batch_id
FROM staging.stg_fx_rate_long
ORDER BY currency_code, rate_type, rate_date DESC;
