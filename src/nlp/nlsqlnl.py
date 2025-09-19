# File: src/nlp/nlsqlnl.py
import json
import re
import sys
import time
import os
from typing import Any, Dict, List, Tuple, Optional

import duckdb
import requests

# ========= DB CONFIG (hard-coded for now; replace with env later) =========
DB_PATH       = "outputs/pkm.duckdb"
DB_TABLE_NAME = "logs_pkm show-save DB files source data"

# ========= env helpers =========
def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def _ollama_endpoint_from_host(host: str) -> str:
    host = host.rstrip("/")
    return host if host.endswith("/api/generate") else host + "/api/generate"

# ========= required env for LLM (fail-fast; no defaults) =========≤
SQL_LLM_HOST    = _require_env("SQL_LLM_HOST")      # e.g., http://ollama:11434
SQL_LLM_MODEL   = _require_env("SQL_LLM_MODEL")     # e.g., llama3.2:3b-instruct-q4_K_M
OLLAMA_URL      = _ollama_endpoint_from_host(SQL_LLM_HOST)

# ========= guardrails =========
NLQ_TIMEOUT_S = 60
NLG_TIMEOUT_S = 60
MAX_ROWS      = 500

# ========= schema (CTE) =========
COLUMNS: List[Dict[str, Any]] = [
    {"name": "User",               "alias": "user_name",        "cast": "TEXT",      "description": "Person who performed the action"},
    {"name": "User ID",            "alias": "user_id",          "cast": "BIGINT",    "description": "Numeric identifier of the actor"},
    {"name": "Subsequence ID",     "alias": "subsequence_id",   "cast": "DOUBLE",    "description": "Within-entry subsequence identifier"},
    {"name": "Message",            "alias": "message",          "cast": "TEXT",      "description": "Full free-text event message"},
    {"name": "Audit Time (UTC)",   "alias": "audit_timestamp",  "cast": "TIMESTAMP", "description": "Event timestamp in UTC", "timestamp": True},
    {"name": "Action",             "alias": "action",           "cast": "TEXT",      "description": "High-level action category"},
    {"name": "Type",               "alias": "entity_type",      "cast": "TEXT",      "description": "Entity type affected"},
    {"name": "Label",              "alias": "label",            "cast": "TEXT",      "description": "Short label or identifier"},
    {"name": "Version",            "alias": "version",          "cast": "TEXT",      "description": "Version string or NA marker"},
]

class NLModuleError(Exception):
    pass

def _build_cte(table_name: str, columns: List[Dict[str, Any]]) -> Tuple[str, List[str], str]:
    if not columns:
        raise NLModuleError("COLUMNS list is empty")

    selects: List[str] = []
    aliases: List[str] = []
    schema_lines: List[str] = []
    ts_count = 0

    for c in columns:
        for k in ("name", "alias", "cast", "description"):
            if k not in c or c[k] in (None, ""):
                raise NLModuleError(f"Column spec missing key '{k}': {c}")
        src = c["name"]
        alias = c["alias"]
        cast = c["cast"]
        desc = c["description"]

        selects.append(f'"{src}"::{cast} AS "{alias}"')
        aliases.append(alias)

        if c.get("timestamp") is True:
            ts_count += 1
            schema_lines.append(f"- {alias} (timestamp): {desc}")
        else:
            schema_lines.append(f"- {alias}: {desc}")

    if ts_count > 1:
        raise NLModuleError("Only one column may be marked timestamp=True")

    # table name may contain spaces; always quote it
    cte = f'WITH t AS (SELECT {", ".join(selects)} FROM "{table_name}")'
    return cte, aliases, "\n".join(schema_lines)

# ---------- extraction helpers ----------
FENCE_JSON   = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE)
FENCE_SQL    = re.compile(r"```sql\s*(.+?)\s*```", re.DOTALL | re.IGNORECASE)
FIRST_OBJECT = re.compile(r"(\{.*\})", re.DOTALL)
FIRST_SELECT = re.compile(r"(?is)\bselect\b.*")  # from first 'select' to end

def _extract_json_block(txt: str) -> Optional[str]:
    if not txt:
        return None
    m = FENCE_JSON.search(txt)
    if m:
        return m.group(1)
    start = txt.find("{"); end = txt.rfind("}")
    if start != -1 and end != -1 and end > start:
        return txt[start:end+1]
    m2 = FIRST_OBJECT.search(txt)
    return m2.group(1) if m2 else None

def _extract_sql_block(txt: str) -> Optional[str]:
    if not txt:
        return None
    m = FENCE_SQL.search(txt)
    if m:
        return m.group(1)
    m2 = FIRST_SELECT.search(txt)
    return m2.group(0).strip() if m2 else None

def _ollama_json(prompt: str, timeout_s: int) -> Dict[str, Any]:
    payload = {"model": SQL_LLM_MODEL, "prompt": prompt, "stream": False}
    r = requests.post(OLLAMA_URL, json=payload, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    txt = data.get("response", "")

    # 1) direct JSON
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict) and "sql" in obj:
            return obj
    except Exception:
        pass

    # 2) fenced/prose JSON
    cand = _extract_json_block(txt)
    if cand:
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict) and "sql" in obj:
                return obj
        except Exception:
            pass

    # 3) SQL block → wrap
    sql_cand = _extract_sql_block(txt)
    if sql_cand:
        return {"sql": sql_cand, "reason": "auto-extracted from model output"}

    raise NLModuleError(f"LLM did not return usable SQL/JSON. RAW:\n{txt[:800]}")

# ---------- SQL guards ----------
_SQL_FORBIDDEN = re.compile(
    r"\b(UPDATE|DELETE|INSERT|MERGE|ALTER|DROP|TRUNCATE|ATTACH|EXPORT|PRAGMA|CALL|CREATE|REPLACE)\b",
    re.I,
)

def _take_first_statement(sql: str) -> str:
    parts = [p.strip() for p in sql.split(";")]
    for p in parts:
        if p:
            return p
    return sql.strip()

def _normalize_from_t(sql: str) -> str:
    """
    Normalize any single-table FROM to 'FROM t' (skip subqueries).
    Handles quoted identifiers.
    """
    # already good?
    if re.search(r"(?is)\bfrom\s+t\b", sql):
        return sql
    # replace first FROM <identifier> where next non-space char isn't '('
    def repl(m: re.Match) -> str:
        before = m.group(1)
        return f"{before}FROM t"
    # pattern: (…)\bfrom\s+<not '(' and not 't'>…
    pattern = re.compile(r"(?is)(.*?\b)from\s+(?!\()(?:(?:\"[^\"]+\"|'[^']+'|`[^`]+`|$begin:math:display$[^$end:math:display$]+\]|[a-zA-Z_][\w\.]*))(?!\s*\()", re.DOTALL)
    return re.sub(pattern, repl, sql, count=1)

def _sanitize_sql(sql: str, max_rows: int) -> str:
    sql = _take_first_statement(sql.strip())
    low = sql.lower()
    if not low.startswith("select") and not low.startswith("with"):
        raise NLModuleError("Only SELECT is allowed")
    if _SQL_FORBIDDEN.search(sql):
        raise NLModuleError("Only read-only SELECT is allowed")

    # If model inlined its own WITH t AS (...) remove it; we provide the outer CTE.
    if re.match(r"(?is)with\s+t\s+as\s*\(", low):
        last_paren = sql.rfind(")")
        if last_paren != -1 and last_paren + 1 < len(sql):
            sql = sql[last_paren + 1:].strip()
            low = sql.lower()

    # Normalize any invented table to 't'
    sql = _normalize_from_t(sql)
    low = sql.lower()

    if " from t" not in low and " from\n t" not in low:
        raise NLModuleError("Query must select from the CTE 't' only")

    if re.search(r"\blimit\s+\d+\b", low) is None:
        sql = f"{sql.rstrip()} LIMIT {int(max_rows)}"
    return sql

def _ollama_text(prompt: str, timeout_s: int) -> str:
    payload = {"model": SQL_LLM_MODEL, "prompt": prompt, "stream": False}
    r = requests.post(OLLAMA_URL, json=payload, timeout=timeout_s)
    r.raise_for_status()
    return r.json().get("response", "").strip()

def _build_sql_prompt(nlq: str, schema_text: str, max_rows: int) -> str:
    return (
        "Translate the QUESTION into a single DuckDB SELECT over the CTE 't'.\n"
        "Rules:\n"
        "- Output JSON with exactly two keys: sql, reason.\n"
        "- Query only FROM t using the columns listed below.\n"
        "- Single statement only. SELECT only. No DDL/DML/PRAGMA/COPY.\n"
        f"- Always include LIMIT <= {max_rows} unless the question specifies a smaller LIMIT.\n"
        "- Prefer standard DuckDB functions (date_trunc, now, interval) when needed.\n"
        "- Do NOT invent table names; the ONLY source is the CTE named t.\n\n"
        "Columns in t:\n"
        f"{schema_text}\n\n"
        "QUESTION:\n"
        f"{nlq}\n\n"
        "Return JSON only:\n"
        "{\"sql\": \"SELECT ... FROM t ...\", \"reason\": \"...\"}"
    )

def _build_summary_prompt(nlq: str, sql: str, cols: List[str], rows: List[List[Any]]) -> str:
    data = {"columns": cols, "rows": rows}
    return (
        "You are a precise data analyst. Explain the result to a non-technical user.\n"
        "Rules:\n"
        "- Be concise and factual. No fluff. No speculation.\n"
        "- Mention any filters/periods/metrics explicitly if present.\n"
        "- If the result set is empty, say so and suggest widening dates or removing filters.\n"
        "- Do NOT invent numbers.\n\n"
        f"QUESTION: {nlq}\n"
        f"SQL: {sql}\n"
        f"RESULT JSON: {json.dumps(data, ensure_ascii=False)}\n"
        "Return plain text only."
    )

def ask(nlq: str) -> Dict[str, Any]:
    """NL -> SQL -> execute -> NL summary."""
    cte_sql, _aliases, schema_text = _build_cte(DB_TABLE_NAME, COLUMNS)

    prompt_sql = _build_sql_prompt(nlq, schema_text, MAX_ROWS)
    t0 = time.time()
    obj = _ollama_json(prompt_sql, NLQ_TIMEOUT_S)
    sql_raw = obj.get("sql", "")
    reason  = obj.get("reason", "")

    sql_checked = _sanitize_sql(sql_raw, MAX_ROWS)
    sql_final   = f"{cte_sql} {sql_checked}"

    con = duckdb.connect(DB_PATH)
    rows = con.execute(sql_final).fetchall()
    cols = [d[0] for d in con.description] if con.description else []
    dt = time.time() - t0

    summary = _ollama_text(_build_summary_prompt(nlq, sql_final, cols, rows), NLG_TIMEOUT_S)

    return {
        "question": nlq,
        "sql": sql_final,
        "reason": reason,
        "seconds_total": round(dt, 3),
        "columns": cols,
        "rows": rows,
        "summary": summary,
    }

if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) if len(sys.argv) >= 2 else input("Ask a question: ").strip()
    out = ask(q)
    print("\n--- SQL ---")
    print(out["sql"])
    print("\n--- ROWS (up to LIMIT) ---")
    print(f"columns={out['columns']}")
    for r in out["rows"]:
        print(r)
    print("\n--- SUMMARY ---")
    print(out["summary"])
    print(f"\n(took {out['seconds_total']}s)")