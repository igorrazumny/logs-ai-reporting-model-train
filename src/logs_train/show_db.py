# File: src/logs_train/show_db.py
# Purpose: Read/print rows from DuckDB (JSON Lines) without touching loader/CLI code.

import os
import sys
import json
import duckdb
import datetime
import decimal
from typing import Any, Dict, List

def _coerce_jsonable(v: Any) -> Any:
    """Coerce DuckDB/Python types to JSON-serializable values."""
    if v is None:
        return None
    # datetimes / dates / times
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        # Use ISO 8601; keep naive datetimes as-is
        return v.isoformat()
    # decimals
    if isinstance(v, decimal.Decimal):
        # Preserve exact text; avoid float rounding surprises
        return str(v)
    # bytes
    if isinstance(v, (bytes, bytearray)):
        # Decode utf-8 if possible, else hex
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    # everything else (int/float/str/bool)
    return v

def _row_to_json(row: tuple, cols: List[str]) -> Dict[str, Any]:
    return {k: _coerce_jsonable(v) for k, v in zip(cols, row)}

def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "outputs/pkm.duckdb"
    limit   = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    if not os.path.exists(db_path):
        print(json.dumps({"error": "DB not found", "db_path": db_path}))
        return 2

    con = duckdb.connect(db_path, read_only=True)
    try:
        # ensure table exists
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='logs_pkm'"
        ).fetchone()[0]
        if not exists:
            print(json.dumps({"error": "Table logs_pkm not found", "db_path": db_path}))
            return 3

        total = con.execute("SELECT COUNT(*) FROM logs_pkm").fetchone()[0]
        print(json.dumps({"info": "logs_pkm", "db_path": db_path, "total_rows": total, "preview_limit": limit}))

        cols = ["ts","actor","actor_display","product","action","type","id","subseq_id","version","message"]
        rows = con.execute(
            "SELECT ts, actor, actor_display, product, action, type, id, subseq_id, version, message "
            "FROM logs_pkm ORDER BY ts NULLS LAST LIMIT ?",
            [limit]
        ).fetchall()

        for r in rows:
            print(json.dumps(_row_to_json(r, cols), ensure_ascii=False))
    finally:
        con.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())