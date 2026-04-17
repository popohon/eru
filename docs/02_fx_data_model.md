# Assessment 2 - FX Data Model Design
## Input Context
- Source file: `fx_rate.xlsx`
- Layout type: **wide crosstab**, not normalized
- Contains two sections:
  - `Closing rate`
  - `Average rate`
- Currency rows: `SGD`, `PHP`, `IDR`, `EUR`, `HKD`
- Date headers are Excel serial numbers; there is also non-date marker text such as `FY2024`

## Modeling Goals
1. Keep full historical FX records.
2. Make joins with loan snapshots simple and performant.
3. Avoid changing source Excel format.
4. Support operational/reporting use cases with consistent definitions.

## Proposed Data Model
## Table: `staging.stg_fx_rate_long`
Columns:
- `rate_date` (DATE) - effective FX date
- `currency_code` (VARCHAR(3)) - ISO-like code from source row labels
- `rate_type` (VARCHAR) - `closing_rate` or `average_rate`
- `rate_to_usd` (NUMERIC) - local currency per 1 USD
- `source_file` (TEXT) - lineage
- `ingested_at` (TIMESTAMPTZ) - load timestamp
- `batch_id` (TEXT) - ingestion batch tracking

Primary key:
- (`rate_date`, `currency_code`, `rate_type`)

## Table: `staging.stg_loanbook_snapshot`
Columns:
- `snapshot_date`, `loan_id`, `requested_principal`, `outstanding_balance`, `status`
- `currency_code` (set from config, default `IDR`)
- lineage fields (`source_file`, `ingested_at`, `batch_id`)

Primary key:
- (`snapshot_date`, `loan_id`)

## Table: `mart.fct_loan_outstanding_usd`
Purpose:
- ready-to-consume fact table for outstanding conversion

Key logic:
- For each loan snapshot row, join to latest available `closing_rate` with:
  - same `currency_code`
  - `fx.rate_date <= snapshot_date`
  - most recent by date (lateral lookup)

Output columns:
- local outstanding
- applied FX rate and date
- USD outstanding

## Why Long Format for FX
Compared to wide monthly columns, long format is:
1. easier to join and filter (`WHERE rate_date`, `WHERE currency_code`, `WHERE rate_type`)
2. easier to validate uniqueness and completeness
3. easier to append new months without schema changes
4. better for marts, BI tools, and reporting SQL

## Transformation Code (Implemented)
FX transformation logic is implemented in:
- `pipeline/etl/fx_rate.py`
- helper date parser in `pipeline/etl/utils.py`

Transformation flow:
1. Read the raw sheet with no header assumptions.
2. Parse valid date headers from row 5, ignoring non-date markers (such as `FY2024`).
3. Detect section labels (`Closing rate`, `Average rate`) and map to `rate_type`.
4. Unpivot currency rows from wide columns into long records.
5. Standardize to output fields: `rate_date`, `currency_code`, `rate_type`, `rate_to_usd`.
6. Drop duplicates and enforce positive rates before loading to staging.
