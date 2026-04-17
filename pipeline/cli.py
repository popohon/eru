from __future__ import annotations

import argparse
import logging

from pipeline import jobs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Erudify local data pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create schemas and tables")

    ingest_fx_parser = subparsers.add_parser("ingest-fx", help="Ingest FX excel file")
    ingest_fx_parser.add_argument("--file-path", required=False, help="Override FX file path")

    ingest_loan_parser = subparsers.add_parser(
        "ingest-loanbook", help="Ingest loanbook excel file"
    )
    ingest_loan_parser.add_argument(
        "--file-path", required=False, help="Override loanbook file path"
    )

    subparsers.add_parser("build-mart", help="Refresh USD mart")
    subparsers.add_parser("validate-fx", help="Run Great Expectations on FX staging")
    subparsers.add_parser(
        "validate-loanbook", help="Run Great Expectations on loanbook staging"
    )
    subparsers.add_parser("validate-mart", help="Run Great Expectations on mart")
    subparsers.add_parser("run-fx-pipeline", help="End-to-end FX pipeline tasks")
    subparsers.add_parser("run-loan-pipeline", help="End-to-end loan pipeline tasks")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init-db":
        jobs.ensure_database_objects()
    elif args.command == "ingest-fx":
        inserted = jobs.run_fx_ingestion(file_path=args.file_path)
        logger.info("FX records upserted: %s", inserted)
    elif args.command == "ingest-loanbook":
        inserted = jobs.run_loanbook_ingestion(file_path=args.file_path)
        logger.info("Loanbook records upserted: %s", inserted)
    elif args.command == "build-mart":
        jobs.build_loan_outstanding_mart()
    elif args.command == "validate-fx":
        logger.info("Validation summary: %s", jobs.validate_fx_staging().to_dict())
    elif args.command == "validate-loanbook":
        logger.info("Validation summary: %s", jobs.validate_loan_staging().to_dict())
    elif args.command == "validate-mart":
        logger.info("Validation summary: %s", jobs.validate_mart().to_dict())
    elif args.command == "run-fx-pipeline":
        jobs.ensure_database_objects()
        jobs.run_fx_ingestion()
        jobs.validate_fx_staging()
        jobs.build_loan_outstanding_mart()
        jobs.validate_mart()
    elif args.command == "run-loan-pipeline":
        jobs.ensure_database_objects()
        jobs.run_loanbook_ingestion()
        jobs.validate_loan_staging()
        jobs.build_loan_outstanding_mart()
        jobs.validate_mart()
    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
