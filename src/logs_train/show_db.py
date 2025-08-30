# Purpose: Read/print rows from DuckDB (JSON Lines), aligned with YAML fields.
# - LIMIT=0 -> print ALL rows (no LIMIT clause)
# - Uses adapters/pkm.yaml (or ADAPTER_YAML env) for: app + fields
# - Table name: logs_{app}
# - Prints a metadata line first, then each row as JSON

import os
import sys
import json
import duckdb
import datetime
import decimal
from typing import Any, Dict, List

import yaml

DEFAULT_ADAPTER_YAML = os.getenv("ADAPTER_YAML", "adapters/pkm.yaml")

def _coerce_jsonable(v: Any) -> Any:
    """Coerce DuckDB/Python types to JSON-serializable values."""
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    return v

def _row_to_json(row: tuple, cols: List[str]) -> Dict[str, Any]:
    return {k: _coerce_jsonable(v) for k, v in zip(cols, row)}

def _read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _quote_ident(ident: str) -> str:
    # DuckDB identifier quoting (handles spaces, parentheses, etc.)
    return '"' + ident.replace('"', '""') + '"'

def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "outputs/pkm.duckdb"
    limit   = int(sys.argv[2]) if len(sys.argv) > 2 else 0  # 0 = ALL

    if not os.path.exists(db_path):
        print(json.dumps({"error": "DB not found", "db_path": db_path}))
        return 2

    # Read adapter YAML for app+fields to align names exactly with the source
    cfg = _read_yaml(DEFAULT_ADAPTER_YAML)
    if "app" not in cfg or not str(cfg["app"]).strip():
        print(json.dumps({"error": "Missing 'app' in YAML", "yaml": DEFAULT_ADAPTER_YAML}))
        return 3
    if "fields" not in cfg or not isinstance(cfg["fields"], list) or not cfg["fields"]:
        print(json.dumps({"error": "Missing or empty 'fields' in YAML", "yaml": DEFAULT_ADAPTER_YAML}))
        return 3

    app = str(cfg["app"]).strip()
    fields: List[str] = [str(c) for c in cfg["fields"]]
    table = f"logs_{app}"

    con = duckdb.connect(db_path, read_only=True)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table.lower()]
        ).fetchone()[0]
        if not exists:
            print(json.dumps({"error": "Table not found", "table": table, "db_path": db_path}))
            return 4

        total = con.execute(f"SELECT COUNT(*) FROM {_quote_ident(table)}").fetchone()[0]
        # Print metadata (always)
        print(json.dumps({
            "info": table,
            "db_path": db_path,
            "total_rows": total,
            "preview_limit": limit,
            "columns": fields
        }))

        # Build SELECT with quoted identifiers so we can use exact field names
        sel_cols = ", ".join(_quote_ident(c) for c in fields)
        sql = f"SELECT {sel_cols} FROM {_quote_ident(table)}"
        params = []
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)

        rows = con.execute(sql, params).fetchall()

        for r in rows:
            print(json.dumps(_row_to_json(r, fields), ensure_ascii=False))
    finally:
        con.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())