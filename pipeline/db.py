from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pipeline.config import settings


def get_engine(db_uri: Optional[str] = None) -> Engine:
    return create_engine(db_uri or settings.warehouse_db_uri, future=True)


def run_sql_file(engine: Engine, sql_file: Path) -> None:
    sql_text = sql_file.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def query_dataframe(engine: Engine, query: str) -> pd.DataFrame:
    with engine.begin() as connection:
        return pd.read_sql(text(query), connection)
