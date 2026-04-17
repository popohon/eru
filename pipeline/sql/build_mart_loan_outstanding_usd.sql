TRUNCATE TABLE mart.fct_loan_outstanding_usd;

INSERT INTO mart.fct_loan_outstanding_usd (
    snapshot_date,
    loan_id,
    status,
    currency_code,
    outstanding_balance_local,
    fx_rate_to_usd,
    fx_rate_date,
    outstanding_balance_usd,
    refreshed_at
)
SELECT
    lb.snapshot_date,
    lb.loan_id,
    lb.status,
    lb.currency_code,
    COALESCE(lb.outstanding_balance, 0)::NUMERIC(20, 2) AS outstanding_balance_local,
    fx_lookup.rate_to_usd,
    fx_lookup.rate_date AS fx_rate_date,
    CASE
        WHEN fx_lookup.rate_to_usd IS NULL OR fx_lookup.rate_to_usd = 0 THEN NULL
        ELSE ROUND((COALESCE(lb.outstanding_balance, 0) / fx_lookup.rate_to_usd)::NUMERIC, 2)
    END AS outstanding_balance_usd,
    NOW() AS refreshed_at
FROM staging.stg_loanbook_snapshot lb
LEFT JOIN LATERAL (
    SELECT
        fx.rate_date,
        fx.rate_to_usd
    FROM staging.stg_fx_rate_long fx
    WHERE fx.currency_code = lb.currency_code
      AND fx.rate_type = 'closing_rate'
      AND fx.rate_date <= lb.snapshot_date
    ORDER BY fx.rate_date DESC
    LIMIT 1
) fx_lookup ON TRUE;
