# File: logs-ai-reporting-model-train/db/init_db.py

import os
import sys
import psycopg
from db.connection import get_connection

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")

def main() -> int:
    if not os.path.isfile(SCHEMA_PATH):
        msg = f"[init_db] schema not found: {SCHEMA_PATH}"
        print(msg, file=sys.stderr)
        return 2

    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            sql = f.read()

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

        print("[init_db] schema applied.")
        return 0

    except Exception as e:
        import traceback
        print(f"[init_db] ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)   # full stack trace in logs
        return 1

if __name__ == "__main__":
    raise SystemExit(main())