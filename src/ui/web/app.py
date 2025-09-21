# Project: logs-ai-reporting-model-train — File: src/ui/web/app.py
import streamlit as st
from ui.admin.app_admin import render_admin
from ui.web.view_chat import view_chat  # adjust if your chat view lives elsewhere

# --- MUST be first Streamlit call on the page ---
def _is_admin_mode() -> bool:
    try:
        params = st.query_params
    except Exception:
        params = st.experimental_get_query_params()
    v = params.get("admin")
    return v in ("admin", ["admin"], "true", ["true"], True)

ADMIN = _is_admin_mode()
PAGE_TITLE = "[ADMIN] BCCA Logs AI Reporting" if ADMIN else "BCCA Logs AI Reporting"
PAGE_ICON = "📊"

st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON)

def main() -> None:
    if ADMIN:
        render_admin()
    else:
        view_chat()

if __name__ == "__main__":
    main()