# Project: logs-ai-reporting-model-train — File: src/web/app.py
import os

import streamlit as st
from admin.app_admin import render_admin  # top-level, eager import

ADMIN_QUERY_KEY = "admin"  # http://host:8501/?admin=admin

def view_chat() -> None:
    st.title("Logs AI — Chat")
    st.caption("User interface (NL→SQL coming later).")
    st.info("Chat UI placeholder. NL→SQL + visualizations will appear here.")

def main() -> None:
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