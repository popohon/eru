# Great Expectations Validation Design
## Why Great Expectations
Great Expectations provides:
1. human-readable and explicit data contracts,
2. reusable checks across staging and marts,
3. integration point for pipeline failure gating.

This project uses GE checks programmatically in `pipeline/validations/ge_validations.py`.

## Validation Scope
## FX staging: `staging.stg_fx_rate_long`
Checks include:
- expected columns and ordering
- non-null key fields (`rate_date`, `currency_code`, `rate_type`)
- allowed `currency_code` domain (`SGD`, `PHP`, `IDR`, `EUR`, `HKD`)
- allowed `rate_type` domain (`closing_rate`, `average_rate`)
- positive FX values (`rate_to_usd > 0`)
- uniqueness of (`rate_date`, `currency_code`, `rate_type`)

## Loan staging: `staging.stg_loanbook_snapshot`
Checks include:
- expected columns and ordering
- non-null keys (`snapshot_date`, `loan_id`)
- status domain (`Submission`, `Activated`, `Closed`)
- positive requested principal
- non-negative outstanding balance
- valid currency code regex
- uniqueness of (`snapshot_date`, `loan_id`)

## Mart: `mart.fct_loan_outstanding_usd`
Checks include:
- expected columns
- non-null keys and FX linkage (`snapshot_date`, `loan_id`, `fx_rate_to_usd`)
- non-negative local and USD outstanding amounts
- uniqueness of (`snapshot_date`, `loan_id`)

## Pipeline Integration Pattern
- Validation runs immediately after staging load and mart build.
- Failure raises runtime exception -> Airflow task fails -> DAG run stops.
- This creates a hard quality gate before downstream consumption.

## Expected Failure Examples
1. FX duplicate key rows on same (`rate_date`, `currency_code`, `rate_type`)
2. Missing/zero FX rates causing invalid conversion
3. Loan rows with unknown status or missing identifiers
4. Mart rows without FX match
