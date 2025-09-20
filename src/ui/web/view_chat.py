# Project: logs-ai-reporting-model-train — File: src/ui/web/view_chat.py
import json
import time
import threading
import streamlit as st

from llm.adapter import call_llm
from nl_sql.utils import (
    extract_sql, strip_fences, looks_like_sql,
    exec_sql, auto_answer, fmt_elapsed
)
from log.events import log_sql_event
from ui.web.chat_skin import inject_chat_css, user_bubble
from ui.web.examples import EXAMPLES_MD

PROMPT_PATH = "prompts/prompt.txt"  # system instruction file for the small LLM

def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def view_chat() -> None:
    inject_chat_css()

    st.title("Logs AI — Chat")
    st.markdown(EXAMPLES_MD)

    # Load the system prompt once
    try:
        system_prompt = _read_file(PROMPT_PATH)
    except Exception as e:
        st.error(f"Cannot read {PROMPT_PATH}: {e}")
        return

    # Render prior turns
    if "history" not in st.session_state:
        st.session_state["history"] = []
    for turn in st.session_state["history"]:
        user_bubble(f"{turn['user']}")
        st.markdown(turn["answer"])

    # Single input (Enter submits)
    user_q = st.chat_input("Ask about the logs…")
    if not user_q:
        return

    # Echo user message (right-aligned)
    user_bubble(f"{user_q}")

    # Live elapsed next to spinner
    t0 = time.perf_counter()
    elapsed_placeholder = st.empty()
    stop_event = threading.Event()

    def _tick():
        while not stop_event.is_set():
            elapsed_placeholder.caption(f"Thinking… {fmt_elapsed(time.perf_counter() - t0)}")
            time.sleep(0.2)

    tick_thread = threading.Thread(target=_tick, daemon=True)
    tick_thread.start()

    raw_a = raw_b = ""
    sql_stmt = ""
    cols, rows = [], []

    try:
        with st.spinner("Checking logs…"):
            # Phase A — NL → SQL
            try:
                raw_a = call_llm(system_prompt, user_q)
                sql_stmt = extract_sql(raw_a)
                if not sql_stmt:
                    ans = "I couldn't interpret that request. Please rephrase or be more specific."
                    st.markdown(ans)
                    st.session_state["history"].append({"user": user_q, "answer": ans})
                    log_sql_event(user_q, "", [], [], 0, error="empty SQL",
                                  raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                    return
            except Exception as e:
                ans = "I couldn't generate a query for that request. Please try rephrasing."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(user_q, "", [], [], 0, error=f"LLM generation error: {e}",
                              raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                return

            # Phase DB — execute SQL (statement timeout handled inside exec_sql)
            try:
                cols, rows = exec_sql(sql_stmt)
            except Exception as e:
                ans = "I couldn’t find an answer for this request. Please try rephrasing."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(user_q, sql_stmt, [], [], 0, error=str(e),
                              raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                return

            if not rows:
                ans = "No matching records found in the logs for this request."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(user_q, sql_stmt, cols, [], 0,
                              raw_a=raw_a, elapsed_total=time.perf_counter() - t0)
                return

            # Phase B — summarize to natural text only
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
                raw_b = call_llm(system_prompt, phase_b_input)
            except Exception:
                raw_b = ""
    finally:
        stop_event.set()
        tick_thread.join(timeout=0.3)
        elapsed_placeholder.empty()

    # Final answer; never show SQL
    answer_text = strip_fences(raw_b).strip()
    if not answer_text or looks_like_sql(answer_text):
        answer_text = auto_answer(cols, rows)

    st.markdown(answer_text)
    st.caption(f"Thought for {fmt_elapsed(time.perf_counter()-t0)}")

    st.session_state["history"].append({"user": user_q, "answer": answer_text})
    log_sql_event(user_q, sql_stmt, cols, rows[:20], len(rows),
                  raw_a=raw_a, raw_b=raw_b, elapsed_total=time.perf_counter() - t0)