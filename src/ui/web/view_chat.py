import json
import time
import threading
import streamlit as st
from decimal import Decimal
from datetime import datetime, date

from llm.adapter import call_llm
from nl_sql.utils import (
    extract_sql, strip_fences, looks_like_sql,
    exec_sql, auto_answer, fmt_elapsed
)
from log.events import log_sql_event
from ui.web.chat_skin import inject_chat_css, user_bubble
from ui.web.examples import EXAMPLES_MD

from streamlit.components.v1 import html as st_html

PROMPT_PATH = "prompts/prompt.txt"  # system instruction file for the small LLM


def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _build_recent_context(history, n: int = 3, max_chars: int = 300) -> str:
    """
    Build a compact conversational context from the last n turns.
    Include user question and assistant answer, trimmed to max_chars total.
    """
    if not history:
        return ""
    turns = history[-n:]
    bullets, total = [], 0
    for t in turns:
        u = (t.get("user") or "").strip()
        a = (t.get("answer") or "").strip()
        if u:
            s = f"- User: {u}"
            if total + len(s) + 1 > max_chars:
                break
            bullets.append(s)
            total += len(s) + 1
        if a:
            s = f"- Answer: {a}"
            if total + len(s) + 1 > max_chars:
                break
            bullets.append(s)
            total += len(s) + 1
    return ("Recent context:\n" + "\n".join(bullets)) if bullets else ""


def _jsonify_rows(rows):
    """Coerce DB row values to JSON-serializable types for Phase B payloads."""

    def coerce(v):
        if isinstance(v, Decimal):
            try:
                return float(v)
            except Exception:
                return str(v)
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    return [[coerce(v) for v in r] for r in rows]


def _scroll_to_bottom(force: bool = False) -> None:
    """
    Scroll to bottom of the page.

    Args:
        force: If True, uses a more aggressive scrolling approach with multiple attempts
    """
    if force:
        # More aggressive approach for when we really need to scroll after LLM response
        scroll_script = """
         <script>
         // Try multiple times to ensure scrolling happens after all content loads
         function scrollToBottom() {
             window.scrollTo(0, document.body.scrollHeight);
             // Also try scrolling the main content area if it exists
             const mainContent = document.querySelector('[data-testid="stAppViewContainer"]');
             if (mainContent) {
                 mainContent.scrollTop = mainContent.scrollHeight;
             }
         }

         // Immediate scroll
         scrollToBottom();

         // Delayed scrolls to handle async content loading
         setTimeout(scrollToBottom, 100);
         setTimeout(scrollToBottom, 300);
         setTimeout(scrollToBottom, 500);
         </script>
         """
    else:
        # Standard single scroll
        scroll_script = """
         <script>
         setTimeout(function(){
             window.scrollTo(0, document.body.scrollHeight);
         }, 0);
         </script>
         """

    st_html(scroll_script, height=0)


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
    _scroll_to_bottom()  # Initial scroll after user input

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
            # Build a compact conversational context from recent turns
            context_block = _build_recent_context(st.session_state.get("history", []), n=3, max_chars=300)

            # Phase A — NL → SQL
            try:
                phase_a_user = (
                                   (context_block + "\n\n") if context_block else ""
                               ) + f"User question:\n{user_q}"
                raw_a = call_llm(system_prompt, phase_a_user)
                sql_stmt = extract_sql(raw_a)
                if not sql_stmt:
                    ans = "I couldn't interpret that request. Please rephrase or be more specific."
                    st.markdown(ans)
                    st.session_state["history"].append({"user": user_q, "answer": ans})
                    log_sql_event(
                        user_q, "", [], [], 0, error="empty SQL",
                        raw_a=raw_a, elapsed_total=time.perf_counter() - t0
                    )
                    # Force scroll on error response
                    _scroll_to_bottom(force=True)
                    return
            except Exception as e:
                ans = "I couldn't generate a query for that request. Please try rephrasing."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(
                    user_q, "", [], [], 0, error=f"LLM generation error: {e}",
                    raw_a=raw_a, elapsed_total=time.perf_counter() - t0
                )
                # Force scroll on error response
                _scroll_to_bottom(force=True)
                return

            # Phase DB — execute SQL (statement timeout handled inside exec_sql)
            try:
                cols, rows = exec_sql(sql_stmt)
            except Exception as e:
                ans = "I couldn't find an answer for this request. Please try rephrasing."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(
                    user_q, sql_stmt, [], [], 0, error=str(e),
                    raw_a=raw_a, elapsed_total=time.perf_counter() - t0
                )
                # Force scroll on error response
                _scroll_to_bottom(force=True)
                return

            if not rows:
                ans = "No matching records found in the logs for this request."
                st.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(
                    user_q, sql_stmt, cols, [], 0,
                    raw_a=raw_a, elapsed_total=time.perf_counter() - t0
                )
                # Force scroll on empty results
                _scroll_to_bottom(force=True)
                return

            # Phase B — summarize to natural text only (send ALL rows; JSON-safe)
            sql_result = {
                "columns": cols,
                "rows": _jsonify_rows(rows),
                "total_rows": len(rows)
            }
            phase_b_input = (
                    system_prompt
                    + "\n\n"
                    + ((context_block + "\n\n") if context_block else "")
                    + "User question:\n" + user_q
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
    st.caption(f"Thought for {fmt_elapsed(time.perf_counter() - t0)}")

    # Force scroll after LLM response is displayed
    _scroll_to_bottom(force=True)

    st.session_state["history"].append({"user": user_q, "answer": answer_text})
    log_sql_event(
        user_q, sql_stmt, cols, rows[:20], len(rows),
        raw_a=raw_a, raw_b=raw_b, elapsed_total=time.perf_counter() - t0
    )


# Alternative: Add a container-based approach if the above doesn't work well enough
def view_chat_with_container() -> None:
    """
    Alternative implementation using a container for better scroll control.
    Use this if the JavaScript approach isn't working reliably.
    """
    inject_chat_css()

    st.title("Logs AI — Chat")
    st.markdown(EXAMPLES_MD)

    # Load the system prompt once
    try:
        system_prompt = _read_file(PROMPT_PATH)
    except Exception as e:
        st.error(f"Cannot read {PROMPT_PATH}: {e}")
        return

    # Create a container for chat messages
    chat_container = st.container()

    with chat_container:
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

    # Process new message in the container
    with chat_container:
        # Echo user message (right-aligned)
        user_bubble(f"{user_q}")

        # Create placeholder for response
        response_placeholder = st.empty()
        caption_placeholder = st.empty()

        # Live elapsed next to spinner
        t0 = time.perf_counter()
        stop_event = threading.Event()

        def _tick():
            while not stop_event.is_set():
                caption_placeholder.caption(f"Thinking… {fmt_elapsed(time.perf_counter() - t0)}")
                time.sleep(0.2)

        tick_thread = threading.Thread(target=_tick, daemon=True)
        tick_thread.start()

        raw_a = raw_b = ""
        sql_stmt = ""
        cols, rows = [], []

        try:
            # [Rest of the processing logic remains the same...]
            # Build a compact conversational context from recent turns
            context_block = _build_recent_context(st.session_state.get("history", []), n=3, max_chars=300)

            # Phase A — NL → SQL
            try:
                phase_a_user = (
                                   (context_block + "\n\n") if context_block else ""
                               ) + f"User question:\n{user_q}"
                raw_a = call_llm(system_prompt, phase_a_user)
                sql_stmt = extract_sql(raw_a)
                if not sql_stmt:
                    ans = "I couldn't interpret that request. Please rephrase or be more specific."
                    response_placeholder.markdown(ans)
                    st.session_state["history"].append({"user": user_q, "answer": ans})
                    log_sql_event(
                        user_q, "", [], [], 0, error="empty SQL",
                        raw_a=raw_a, elapsed_total=time.perf_counter() - t0
                    )
                    _scroll_to_bottom(force=True)
                    return
            except Exception as e:
                ans = "I couldn't generate a query for that request. Please try rephrasing."
                response_placeholder.markdown(ans)
                st.session_state["history"].append({"user": user_q, "answer": ans})
                log_sql_event(
                    user_q, "", [], [], 0, error=f"LLM generation error: {e}",
                    raw_a=raw_a, elapsed_total=time.perf_counter() - t0
                )
                _scroll_to_bottom(force=True)
                return

            # [Continue with rest of processing...]
            # Phase DB and Phase B logic here...

        finally:
            stop_event.set()
            tick_thread.join(timeout=0.3)
            caption_placeholder.empty()

        # Display final answer
        response_placeholder.markdown(answer_text)
        caption_placeholder.caption(f"Thought for {fmt_elapsed(time.perf_counter() - t0)}")

    # Force scroll after everything is rendered
    _scroll_to_bottom(force=True)

    st.session_state["history"].append({"user": user_q, "answer": answer_text})
    log_sql_event(
        user_q, sql_stmt, cols, rows[:20], len(rows),
        raw_a=raw_a, raw_b=raw_b, elapsed_total=time.perf_counter() - t0
    )