from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    warehouse_db_uri: str
    fx_rate_filepath: str
    loanbook_filepath: str
    local_currency_code: str


def load_settings() -> Settings:
    return Settings(
        warehouse_db_uri=os.getenv(
            "WAREHOUSE_DB_URI",
            "postgresql+psycopg2://airflow:airflow@localhost:5432/warehouse",
        ),
        fx_rate_filepath=os.getenv("FX_RATE_FILEPATH", "fx_rate.xlsx"),
        loanbook_filepath=os.getenv("LOANBOOK_FILEPATH", "loanbook.xlsx"),
        local_currency_code=os.getenv("LOCAL_CURRENCY_CODE", "IDR"),
    )


settings = load_settings()
