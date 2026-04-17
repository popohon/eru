from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Optional

import pandas as pd

EXCEL_EPOCH = datetime(1899, 12, 30)
NUMERIC_REGEX = re.compile(r"^-?[0-9]+(\.[0-9]+)?$")


def parse_excel_like_date(value: Any) -> Optional[date]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        return (EXCEL_EPOCH + timedelta(days=float(value))).date()
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if NUMERIC_REGEX.match(normalized):
            return (EXCEL_EPOCH + timedelta(days=float(normalized))).date()
        parsed = pd.to_datetime(normalized, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()
    return None
