# Project: logs-ai-reporting-model-train — File: src/ingest/xlsx2db.py

import os
import math
import typing
import pandas as pd
import psycopg
from db.connection import get_connection

# Explicit constants (no defaults)
UPLOAD_DIR = "/app/data/uploads"
TABLE_NAME = "logs_pkm"
BATCH_SIZE = 1000  # explicit constant for executemany batch

# Columns in target table, in order
COLUMNS: typing.List[str] = [
    "user_id","id","subseq_id","message","audit_time","action_raw","type","label","version",
    "recipe_id","recipe_name","material_name","material_id","name1","name2","username",
    "action_derived","session_start","session_end","session_duration",
]

def _coerce_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

def _coerce_int(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        # keep NaN → None; non-numeric → None
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

def _rows_from_df(df: pd.DataFrame) -> typing.Iterable[typing.Tuple[typing.Any, ...]]:
    # Ensure all expected columns exist; extra columns are ignored
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    # Order columns and yield tuples
    ordered = df[COLUMNS]
    for _, row in ordered.iterrows():
        yield tuple(None if pd.isna(v) else v for v in row.tolist())

def _insert_rows(conn: psycopg.Connection, rows: typing.Iterable[typing.Tuple[typing.Any, ...]], total: int) -> int:
    sql = f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})"
    inserted = 0
    with conn.cursor() as cur:
        batch: typing.List[typing.Tuple[typing.Any, ...]] = []
        for r in rows:
            batch.append(r)
            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            inserted += len(batch)
    return inserted

def ingest_folder() -> dict:
    """
    Scan UPLOAD_DIR for .xlsx files and insert into logs_pkm.
    Returns summary dict: {'files': N, 'rows': R}
    """
    if not os.path.isdir(UPLOAD_DIR):
        return {"files": 0, "rows": 0, "note": f"missing upload dir {UPLOAD_DIR}"}

    files = [os.path.join(UPLOAD_DIR, f) for f in os.listdir(UPLOAD_DIR) if f.lower().endswith(".xlsx")]
    files.sort()
    total_rows = 0
    processed = 0

    with get_connection() as conn:
        for path in files:
            # read each workbook as strings; coerce selected columns after
            df = pd.read_excel(path, dtype="string", engine="openpyxl")
            # normalize header shape (lowercase, underscores) if needed
            df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

            # type coercions for timestamp/int columns
            _coerce_datetime(df, "audit_time")
            _coerce_datetime(df, "session_start")
            _coerce_datetime(df, "session_end")
            _coerce_int(df, "session_duration")

            rows_iter = _rows_from_df(df)
            inserted = _insert_rows(conn, rows_iter, len(df))
            conn.commit()

            total_rows += inserted
            processed += 1

    return {"files": processed, "rows": total_rows, "dir": UPLOAD_DIR}