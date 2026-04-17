# Thought Process, Tool Selection, and Tradeoffs
## 1) Problem Framing
The assignment asks for a solution that is:
- robust and cost-efficient,
- maintainable by a small team,
- able to handle mixed sources (DB + Excel),
- suitable for recurring reports and ad-hoc analytics,
- and explicit about design rationale.

So the architecture needs to prioritize **clarity, reliability, and low operational burden**.

## 2) Why These Tools
## Docker Compose
- keeps environment reproducible on any developer machine,
- avoids cloud dependency,
- supports end-to-end demonstration in one command.

## PostgreSQL
- widely adopted, stable, and familiar,
- strong fit for both source context and warehouse-like marts,
- enough for assignment scale without introducing unnecessary complexity.

## Apache Airflow
- standard for production orchestration,
- clear scheduling semantics for monthly + daily jobs,
- retries, logging, dependency management, and observability in one place.

## Python + Pandas
- easiest way to parse non-standard Excel layout without changing source file,
- fast iteration for transformation logic and edge-case handling.

## Great Expectations
- declarative and auditable validation logic,
- supports fail-fast quality gate pattern,
- helps protect regulatory and BI consumers from bad data.

## 3) Key Design Decisions
1. **Normalize FX from wide to long format** to support joins and historical storage.
2. **Keep both rate types** (`closing_rate` and `average_rate`) for flexibility.
3. **Use closing rate for outstanding conversion** in mart because this is usually aligned with end-of-day balance translation.
4. **As-of FX join logic** (`fx_date <= snapshot_date`, latest available) to handle dates with no direct FX row.
5. **Idempotent writes** using upsert keys to support safe re-runs.

## 4) Scheduling Decision Details
The monthly FX update schedule uses `1-4` day window instead of strict day 1 to match the operational reality that files can arrive late after month-end close.

Daily loanbook schedule runs separately so reporting can still refresh even when monthly FX file is unchanged.

## 5) Limitations and Future Improvements
Current implementation is assignment-focused and local-first. In production, I would extend with:
1. separate raw/bronze storage before staging,
2. secrets manager integration,
3. CI tests + lint checks in pipeline,
4. schema evolution controls,
5. data freshness alerting and SLA dashboards,
6. partitioning strategy for very large snapshot tables.

## 6) How To Read This Submission
1. Start with `docs/01_assignment_architecture.md`.
2. Review `docs/02_fx_data_model.md` and `docs/03_pipeline_orchestration.md`.
3. Review quality controls in `docs/04_data_quality_with_gx.md`.
4. Use `README.md` to run the stack end-to-end.
