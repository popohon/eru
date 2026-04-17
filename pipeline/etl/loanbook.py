from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.etl.utils import parse_excel_like_date


def _normalize_status(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    return text_value.title()


def transform_loanbook_file(file_path: str | Path, currency_code: str) -> pd.DataFrame:
    source_path = Path(file_path)
    loanbook = pd.read_excel(source_path, sheet_name=0, engine="openpyxl")
    loanbook.columns = [str(column).strip().lower() for column in loanbook.columns]

    required_columns = [
        "snapshot_date",
        "loan_id",
        "requested_principal",
        "outstanding_balance",
        "status",
    ]
    missing_columns = set(required_columns).difference(loanbook.columns)
    if missing_columns:
        raise ValueError(
            f"Missing expected columns in loanbook file {source_path}: {sorted(missing_columns)}"
        )

    cleaned = loanbook[required_columns].copy()
    cleaned["snapshot_date"] = cleaned["snapshot_date"].apply(parse_excel_like_date)
    cleaned["loan_id"] = cleaned["loan_id"].apply(
        lambda value: str(value).strip().upper()
        if value is not None and not (isinstance(value, float) and pd.isna(value))
        else None
    )
    cleaned["requested_principal"] = pd.to_numeric(cleaned["requested_principal"], errors="coerce")
    cleaned["outstanding_balance"] = pd.to_numeric(cleaned["outstanding_balance"], errors="coerce")
    cleaned["status"] = cleaned["status"].apply(_normalize_status)
    cleaned["currency_code"] = currency_code.upper()

    cleaned = cleaned[cleaned["snapshot_date"].notna()]
    cleaned = cleaned[cleaned["loan_id"].notna()]
    cleaned = cleaned[cleaned["loan_id"].str.len() > 0]
    cleaned = cleaned.sort_values(["snapshot_date", "loan_id"]).drop_duplicates(
        subset=["snapshot_date", "loan_id"], keep="last"
    )

    ordered_columns = [
        "snapshot_date",
        "loan_id",
        "requested_principal",
        "outstanding_balance",
        "status",
        "currency_code",
    ]
    return cleaned[ordered_columns].reset_index(drop=True)
