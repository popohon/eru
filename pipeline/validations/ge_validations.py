from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import great_expectations as gx
import pandas as pd


@dataclass
class ValidationSummary:
    asset_name: str
    success: bool
    total_expectations: int
    successful_expectations: int
    failed_expectations: List[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "asset_name": self.asset_name,
            "success": self.success,
            "total_expectations": self.total_expectations,
            "successful_expectations": self.successful_expectations,
            "failed_expectations": self.failed_expectations,
        }


def _build_summary(asset_name: str, results: List[tuple[str, Dict[str, object]]]) -> ValidationSummary:
    total_expectations = len(results)
    successful_expectations = sum(
        1 for _, result in results if bool(result.get("success"))
    )
    failed_expectations = [
        expectation_name
        for expectation_name, result in results
        if not bool(result.get("success"))
    ]
    return ValidationSummary(
        asset_name=asset_name,
        success=successful_expectations == total_expectations,
        total_expectations=total_expectations,
        successful_expectations=successful_expectations,
        failed_expectations=failed_expectations,
    )


def _empty_dataset_summary(asset_name: str) -> ValidationSummary:
    return ValidationSummary(
        asset_name=asset_name,
        success=False,
        total_expectations=1,
        successful_expectations=0,
        failed_expectations=["dataset_not_empty"],
    )


def validate_fx_staging_dataframe(dataframe: pd.DataFrame) -> ValidationSummary:
    asset_name = "staging.stg_fx_rate_long"
    if dataframe.empty:
        return _empty_dataset_summary(asset_name)

    validator = gx.from_pandas(dataframe)
    expected_columns = [
        "rate_date",
        "currency_code",
        "rate_type",
        "rate_to_usd",
        "source_file",
        "ingested_at",
        "batch_id",
    ]
    results = [
        (
            "table_columns_match_order",
            validator.expect_table_columns_to_match_ordered_list(expected_columns),
        ),
        ("rate_date_not_null", validator.expect_column_values_to_not_be_null("rate_date")),
        (
            "currency_code_not_null",
            validator.expect_column_values_to_not_be_null("currency_code"),
        ),
        (
            "rate_type_not_null",
            validator.expect_column_values_to_not_be_null("rate_type"),
        ),
        (
            "currency_code_set",
            validator.expect_column_values_to_be_in_set(
                "currency_code", ["SGD", "PHP", "IDR", "EUR", "HKD"]
            ),
        ),
        (
            "rate_type_set",
            validator.expect_column_values_to_be_in_set(
                "rate_type", ["closing_rate", "average_rate"]
            ),
        ),
        (
            "rate_positive",
            validator.expect_column_values_to_be_between(
                "rate_to_usd", min_value=0, strict_min=True
            ),
        ),
        (
            "compound_key_unique",
            validator.expect_compound_columns_to_be_unique(
                ["rate_date", "currency_code", "rate_type"]
            ),
        ),
    ]
    return _build_summary(asset_name, results)


def validate_loan_staging_dataframe(dataframe: pd.DataFrame) -> ValidationSummary:
    asset_name = "staging.stg_loanbook_snapshot"
    if dataframe.empty:
        return _empty_dataset_summary(asset_name)

    validator = gx.from_pandas(dataframe)
    expected_columns = [
        "snapshot_date",
        "loan_id",
        "requested_principal",
        "outstanding_balance",
        "status",
        "currency_code",
        "source_file",
        "ingested_at",
        "batch_id",
    ]
    results = [
        (
            "table_columns_match_order",
            validator.expect_table_columns_to_match_ordered_list(expected_columns),
        ),
        (
            "snapshot_date_not_null",
            validator.expect_column_values_to_not_be_null("snapshot_date"),
        ),
        ("loan_id_not_null", validator.expect_column_values_to_not_be_null("loan_id")),
        ("status_not_null", validator.expect_column_values_to_not_be_null("status")),
        (
            "status_allowed",
            validator.expect_column_values_to_be_in_set(
                "status", ["Submission", "Activated", "Closed"]
            ),
        ),
        (
            "requested_principal_positive",
            validator.expect_column_values_to_be_between(
                "requested_principal", min_value=0, strict_min=True
            ),
        ),
        (
            "outstanding_balance_non_negative",
            validator.expect_column_values_to_be_between(
                "outstanding_balance", min_value=0
            ),
        ),
        (
            "currency_code_format",
            validator.expect_column_values_to_match_regex("currency_code", "^[A-Z]{3}$"),
        ),
        (
            "compound_key_unique",
            validator.expect_compound_columns_to_be_unique(["snapshot_date", "loan_id"]),
        ),
    ]
    return _build_summary(asset_name, results)


def validate_mart_dataframe(dataframe: pd.DataFrame) -> ValidationSummary:
    asset_name = "mart.fct_loan_outstanding_usd"
    if dataframe.empty:
        return _empty_dataset_summary(asset_name)

    validator = gx.from_pandas(dataframe)
    expected_columns = [
        "snapshot_date",
        "loan_id",
        "status",
        "currency_code",
        "outstanding_balance_local",
        "fx_rate_to_usd",
        "fx_rate_date",
        "outstanding_balance_usd",
        "refreshed_at",
    ]
    results = [
        (
            "table_columns_match_order",
            validator.expect_table_columns_to_match_ordered_list(expected_columns),
        ),
        (
            "snapshot_date_not_null",
            validator.expect_column_values_to_not_be_null("snapshot_date"),
        ),
        ("loan_id_not_null", validator.expect_column_values_to_not_be_null("loan_id")),
        (
            "fx_rate_to_usd_not_null",
            validator.expect_column_values_to_not_be_null("fx_rate_to_usd"),
        ),
        (
            "outstanding_local_non_negative",
            validator.expect_column_values_to_be_between(
                "outstanding_balance_local", min_value=0
            ),
        ),
        (
            "outstanding_usd_non_negative",
            validator.expect_column_values_to_be_between(
                "outstanding_balance_usd", min_value=0
            ),
        ),
        (
            "compound_key_unique",
            validator.expect_compound_columns_to_be_unique(["snapshot_date", "loan_id"]),
        ),
    ]
    return _build_summary(asset_name, results)
