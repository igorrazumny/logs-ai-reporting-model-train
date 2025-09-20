# Project: logs-ai-reporting-model-train — File: src/web/app.py

import os
import io
import zipfile
import traceback
import streamlit as st

# ---- Config (explicit constants, no defaults) ----
UPLOAD_DIR = "/app/data/uploads"  # persisted via compose bind-mount
ADMIN_QUERY_KEY = "admin"         # URL like: http://host:8501/?admin=admin

# ---- Views ----
def view_chat() -> None:
    st.title("Logs AI — Chat")
    st.caption("User interface (NL→SQL coming later).")
    st.info("Chat UI placeholder. NL→SQL + visualizations will appear here.")

def view_admin() -> None:
    st.title("Logs AI — Admin")
    st.caption("DB init and bulk file upload")

    # Try to import the init module so we can show the exact schema path and apply it.
    init_db_main = None
    schema_path = ""
    schema_exists = False
    import_error = None
    try:
        import db.init_db as _init
        init_db_main = _init.main
        schema_path = _init.SCHEMA_PATH
        schema_exists = os.path.isfile(schema_path)
    except Exception as e:
        import_error = e

    # ---- Schema init block ----
    st.subheader("1) Initialize / Re-apply DB schema")

    if import_error is not None:
        st.error(f"init_db module not available: {import_error}")
    else:
        st.caption(f"schema path: {schema_path}")
        st.caption(f"schema exists: {schema_exists}")

    if st.button("Apply schema.sql now"):
        if init_db_main is None:
            st.error("init_db module not available in container.")
        else:
            try:
                rc = init_db_main()
                if rc == 0:
                    st.success("Schema applied.")
                else:
                    st.error(f"Schema apply failed (exit {rc}). Check container logs.")
            except Exception as e:
                st.error(f"Schema apply raised exception: {e}")
                st.code(traceback.format_exc())

    # ---- Upload block ----
    st.subheader("2) Upload parsed XLSX (or ZIP of XLSX)")
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    uploaded = st.file_uploader(
        "Choose .xlsx or .zip",
        type=["xlsx", "zip"],
        accept_multiple_files=True
    )

    saved_paths = []
    errors = []

    if uploaded:
        for up in uploaded:
            try:
                data = up.read()
                name = os.path.basename(up.name)

                if name.lower().endswith(".zip"):
                    try:
                        with zipfile.ZipFile(io.BytesIO(data)) as zf:
                            for info in zf.infolist():
                                # save only XLSX entries
                                if not info.filename.lower().endswith(".xlsx"):
                                    continue
                                dest = os.path.join(UPLOAD_DIR, os.path.basename(info.filename))
                                # ensure parent dir exists
                                os.makedirs(os.path.dirname(dest), exist_ok=True)
                                with zf.open(info) as src, open(dest, "wb") as out:
                                    out.write(src.read())
                                saved_paths.append(dest)
                    except zipfile.BadZipFile:
                        errors.append(f"Bad ZIP: {name}")
                elif name.lower().endswith(".xlsx"):
                    dest = os.path.join(UPLOAD_DIR, name)
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    with open(dest, "wb") as out:
                        out.write(data)
                    saved_paths.append(dest)
                else:
                    errors.append(f"Unsupported file: {name}")
            except Exception as e:
                errors.append(f"{up.name}: {e}")

        if saved_paths:
            st.success(f"Saved {len(saved_paths)} file(s) to {UPLOAD_DIR}")
            st.code("\n".join(saved_paths))

        if errors:
            st.error("Some files failed:")
            st.code("\n".join(errors))

    st.caption("Next: add an 'Ingest to Postgres' button to load staged XLSX into logs_pkm.")

# ---- Router ----
def main() -> None:
    # Read admin token from env and querystring
    admin_token = os.getenv("ADMIN_TOKEN")
    # Streamlit ≥1.30: st.query_params; earlier versions: st.experimental_get_query_params()
    qp = getattr(st, "query_params", None)
    if qp is None:
        qp_dict = st.experimental_get_query_params()
        admin_param = qp_dict.get(ADMIN_QUERY_KEY, [None])
        admin_param = admin_param[0] if admin_param else None
    else:
        admin_param = qp.get(ADMIN_QUERY_KEY, None)

    # Route
    if admin_token and admin_param == admin_token:
        view_admin()
        return
    view_chat()

if __name__ == "__main__":
    main()