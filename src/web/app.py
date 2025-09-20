# Project: logs-ai-reporting-model-train — File: src/web/app.py
import os
import json
import time
import datetime
from pathlib import Path
import threading
import re

import streamlit as st
import requests
import psycopg

from admin.app_admin import render_admin
from db.connection import get_connection  # proven DB connector

# ---------- explicit constants (no defaults) ----------
PROMPT_PATH = "prompts/prompt.txt"            # system instruction file for the small LLM
ENV_LLM_HOST = "LLM_HOST"                     # e.g., http://ollama:11434
ENV_LLM_MODEL = "LLM_MODEL"                   # e.g., llama3:8b
ADMIN_QUERY_KEY = "admin"                     # routing only; we never show SQL
LOG_PATH = Path("outputs/logs/chat_sql.log")  # JSONL; one event per line

# Time guards
LLM_TIMEOUT_SECS = 300        # per LLM call (5 min)
DB_TIMEOUT_MILLIS = 300000    # Postgres statement_timeout (5 min)

# ---------- helpers ----------
def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def _llm_call(system_prompt: str, user_query: str) -> str:
    """POST to local HTTP LLM endpoint that accepts {model, system, prompt} and returns text."""
    host = os.getenv(ENV_LLM_HOST)
    model = os.getenv(ENV_LLM_MODEL)
    if not host or not model:
        raise RuntimeError(f"Missing {ENV_LLM_HOST} or {ENV_LLM_MODEL}.")
    body = {
        "model": model,
        "system": system_prompt,
        "prompt": user_query,
        "stream": False,
        "options": {}
    }
    r = requests.post(f"{host}/api/generate", json=body, timeout=LLM_TIMEOUT_SECS)
    r.raise_for_status()
    data = r.json()
    return data.get("response", "")

def _strip_fences(s: str) -> str:
    return s.replace("```sql", "").replace("```SQL", "").replace("```", "").strip()

def _extract_sql(text: str) -> str:
    """Extract a single SQL statement from possible fenced output; strip stray fences/semicolons."""
    if not text:
        return ""
    t = text.strip()
    if "```" in t:
        parts = t.split("```")
        for i in range(len(parts) - 1):
            block = parts[i + 1]
            if block.lstrip().lower().startswith("sql"):
                candidate = (block.split("\n", 1)[1] if "\n" in block else "").strip()
                return candidate.replace("```", "").strip().rstrip(";")
        return parts[1].replace("```", "").strip().rstrip(";")
    return t.replace("```", "").strip().rstrip(";")

def _looks_like_sql(s: str) -> bool:
    if not s:
        return False
    t = s.strip().lower()
    if "select " in t and " from " in t:
        return True
    if t.startswith(("with ", "select ")):
        return True
    return False

def _exec_sql(sql: str):
    """Execute read-only SQL and return fetched rows + column names with a statement timeout."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = {DB_TIMEOUT_MILLIS};")
            cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
            return cols, rows

def _fmt_elapsed(seconds: float) -> str:
    """Return human-friendly elapsed time: '59s', '5m 09s', '1h 03m 10s'."""
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"

def _auto_answer(cols, rows) -> str:
    """
    Generate a natural answer from DB rows if the model replies with SQL instead of text.
    Simple patterns first; generic fallback otherwise.
    """
    try:
        if len(rows) == 1 and len(cols) == 1 and isinstance(rows[0][0], (int, float)):
            c = cols[0].lower()
            val = rows[0][0]
            if c == "total_records":
                return f"There are **{val:,}** records in the system."
            if c == "total_recipes":
                return f"There are **{val:,}** distinct recipes in the system."
            # generic numeric one-cell
            return f"Result: **{val:,}** ({cols[0]})."
        # usernames list
        if len(cols) == 1 and cols[0].lower() == "username":
            items = [str(r[0]) for r in rows[:20]]
            total = len(rows)
            shown = min(20, total)
            return f"Found **{total}** users. Showing {shown}:\n- " + "\n- ".join(items)
        # generic table fallback
        total = len(rows)
        shown = min(5, total)
        preview = []
        for r in rows[:shown]:
            pairs = ", ".join(f"{cols[i]}={r[i]}" for i in range(len(cols)))
            preview.append(f"- {pairs}")
        return f"Found **{total}** rows. Preview:\n" + "\n".join(preview)
    except Exception:
        return "Here’s what I found based on the logs."

def _log_sql_event(user_q: str,
                   sql: str,
                   cols: list[str] | None,
                   rows_sample: list[tuple] | None,
                   total_rows: int | None,
                   error: str | None = None,
                   raw_a: str | None = None,
                   raw_b: str | None = None,
                   elapsed_total: float | None = None) -> None:
    """Append one JSON line to outputs/logs/chat_sql.log with query + result sample + raw LLM outputs."""
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
            "user_query": user_q,
            "sql": sql,
            "columns": cols or [],
            "row_count": total_rows if total_rows is not None else 0,
            "rows_sample": rows_sample or [],
            "error": error or "",
            "model_raw_phase_a": raw_a or "",
            "model_raw_phase_b": raw_b or "",
            "elapsed_human": _fmt_elapsed(elapsed_total or 0.0),
            "elapsed_seconds": round(elapsed_total or 0.0, 3),
        }
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass  # never break UI on logging failure

# ---------- chat view ----------
def view_chat() -> None:
    st.title("Logs AI — Chat")
    st.markdown(
        "I help you explore **BCCA logs**. Ask natural questions and I’ll check the logs "
        "and summarize what I find.\n\n"
        "**Examples:**\n"
        "- How much time did `<user>` spend in Q2 2024 on Trastuzumab recipe families?\n"
        "- How many unique users worked on Trastuzumab recipes?\n"
        "- List all users who worked on Trastuzumab recipes.\n"
        "- List 10 users with the most activity last month.\n"
        "- Find the user similar to `<name>` and show their total time this year.\n"
        "- How much time did `<user>` spend working on recipes overall?\n"
        "- How much time did `<user>` spend in the last 5 months?\n"
        "- Top 10 recipes by number of events in 2025.\n"
        "- Total time on 'Tocilizumab' between 2025-07-01 and 2025-09-30.\n"
        "- How many distinct recipes exist in the database?"
    )

    try:
        system_prompt = _read_file(PROMPT_PATH)
    except Exception as e:
        st.error(f"Cannot read {PROMPT_PATH}: {e}")
        return

    if "history" not in st.session_state:
        st.session_state["history"] = []
    for turn in st.session_state["history"]:
        st.markdown(f"**You:** {turn['user']}")
        st.markdown(turn["answer"])

    user_q = st.chat_input("Ask about the logs…")
    if not user_q:
        return

    st.markdown(f"**You:** {user_q}")

    # Live elapsed next to spinner
    t0 = time.perf_counter()
    elapsed_placeholder = st.empty()
    stop_event = threading.Event()

    def _tick():
        while not stop_event.is_set():
            elapsed_placeholder.caption(f"Thinking… {_fmt_elapsed(time.perf_counter() - t0)}")
            time.sleep(0.2)

    tick_thread = threading.Thread(target=_tick, daemon=True)
    tick_thread.start()

    # Full processing under spinner
    raw_a = ""
    raw_b = ""
    sql_stmt = ""
    cols, rows = [], []

    try:
        with st.spinner("Checking logs…"):
            # Phase A — NL → SQL
            try:
                raw_a = _llm_call(system_prompt, user_q)
                sql_stmt = _extract_sql(raw_a)
                if not sql_stmt:
                    ans = "I couldn't interpret that request. Please rephrase or be more specific."
                    st.markdown(ans)
                    st.session_state["history"].append({"user": user_q, "answer": ans})
                    _log_sql_event(user_q, "", [], [], 0, error="empty SQL",
                                   raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                    return
            except Exception as e:
                ans = "I couldn't generate a query for that request. Please try rephrasing."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                _log_sql_event(user_q, "", [], [], 0, error=f"LLM generation error: {e}",
                               raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                return

            # Phase DB — execute SQL
            try:
                cols, rows = _exec_sql(sql_stmt)
            except Exception as e:
                ans = "I couldn’t find an answer for this request. Please try rephrasing."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                _log_sql_event(user_q, sql_stmt, [], [], 0, error=str(e),
                               raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                return

            if not rows:
                ans = "No matching records found in the logs for this request."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                _log_sql_event(user_q, sql_stmt, cols, [], 0,
                               raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                return

            # Phase B — summarize
            MAX_ROWS_TO_SEND = 200
            payload_rows = rows[:MAX_ROWS_TO_SEND]
            sql_result = {"columns": cols, "rows": payload_rows, "total_rows": len(rows)}
            phase_b_input = (
                system_prompt
                + "\n\nUser question:\n" + user_q
                + "\n\nsql_result (JSON):\n" + json.dumps(sql_result, ensure_ascii=False)
                + "\n\nReturn only a short natural-language answer (no code fences, no SQL)."
            )
            try:
                raw_b = _llm_call(system_prompt, phase_b_input)
            except Exception as e:
                raw_b = ""

    finally:
        stop_event.set()
        tick_thread.join(timeout=0.2)
        elapsed_placeholder.empty()

    # Build final answer, sanitizing the model’s Phase-B output
    answer_text = _strip_fences(raw_b).strip()
    if not answer_text or _looks_like_sql(answer_text):
        # Model replied with SQL or nothing → craft human answer from rows
        answer_text = _auto_answer(cols, rows)

    st.markdown(answer_text)
    st.caption(f"Thought for {_fmt_elapsed(time.perf_counter()-t0)}")

    st.session_state["history"].append({
        "user": user_q,
        "answer": answer_text
    })

    _log_sql_event(user_q, sql_stmt, cols, rows[:20], len(rows),
                   raw_a=raw_a, raw_b=raw_b, elapsed_total=time.perf_counter() - t0)

def main() -> None:
    # Route: Admin by token => Admin UI; else Chat
    admin_token = os.getenv("ADMIN_TOKEN")
    qp = getattr(st, "query_params", None)
    if qp is None:
        q = st.experimental_get_query_params()
        admin_param = (q.get(ADMIN_QUERY_KEY, [None]) or [None])[0]
    else:
        admin_param = qp.get(ADMIN_QUERY_KEY, None)

    if admin_token and admin_param == admin_token:
        render_admin()
        return
    view_chat()

if __name__ == "__main__":
    main()