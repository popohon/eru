from __future__ import annotations

import re
from pathlib import Path
from typing import Dict

import pandas as pd

from pipeline.etl.utils import parse_excel_like_date

RATE_SECTION_MAP = {
    "closing rate": "closing_rate",
    "average rate": "average_rate",
}
CURRENCY_REGEX = re.compile(r"^[A-Z]{3}$")
HEADER_ROW_INDEX = 4
LABEL_COLUMN_INDEX = 1
FIRST_DATE_COLUMN_INDEX = 2
FIRST_DATA_ROW_INDEX = 5


def _extract_date_columns(raw_df: pd.DataFrame) -> Dict[int, object]:
    date_columns: Dict[int, object] = {}
    header_row = raw_df.iloc[HEADER_ROW_INDEX, FIRST_DATE_COLUMN_INDEX:]
    for column_index, value in header_row.items():
        parsed_date = parse_excel_like_date(value)
        if parsed_date:
            date_columns[column_index] = parsed_date
    return date_columns


def transform_fx_rate_file(file_path: str | Path) -> pd.DataFrame:
    source_path = Path(file_path)
    raw_df = pd.read_excel(source_path, sheet_name=0, header=None, engine="openpyxl")
    date_columns = _extract_date_columns(raw_df)
    if not date_columns:
        raise ValueError(f"No valid date columns could be parsed from {source_path}")

    records = []
    current_rate_type = None
    for row_index in range(FIRST_DATA_ROW_INDEX, raw_df.shape[0]):
        label_value = raw_df.iat[row_index, LABEL_COLUMN_INDEX]
        if pd.isna(label_value):
            continue

        label = str(label_value).strip()
        if not label:
            continue

        mapped_rate_type = RATE_SECTION_MAP.get(label.lower())
        if mapped_rate_type:
            current_rate_type = mapped_rate_type
            continue
        if not current_rate_type:
            continue

        currency_code = label.upper()
        if not CURRENCY_REGEX.match(currency_code):
            continue

        for column_index, rate_date in date_columns.items():
            rate_value = raw_df.iat[row_index, column_index]
            if pd.isna(rate_value):
                continue
            numeric_rate = pd.to_numeric(rate_value, errors="coerce")
            if pd.isna(numeric_rate):
                continue

            records.append(
                {
                    "rate_date": rate_date,
                    "currency_code": currency_code,
                    "rate_type": current_rate_type,
                    "rate_to_usd": float(numeric_rate),
                }
            )

    fx_rates = pd.DataFrame(records)
    if fx_rates.empty:
        raise ValueError(f"No FX records were produced from {source_path}")

    fx_rates = fx_rates[fx_rates["rate_to_usd"] > 0]
    fx_rates = fx_rates.drop_duplicates(
        subset=["rate_date", "currency_code", "rate_type"], keep="last"
    )
    fx_rates = fx_rates.sort_values(["rate_date", "currency_code", "rate_type"]).reset_index(
        drop=True
    )
    return fx_rates
