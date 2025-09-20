# Project: logs-ai-reporting-model-train — File: src/admin/app_admin.py

import os
import io
import zipfile
import traceback
import streamlit as st

from db.init_db import main as init_db_main

UPLOAD_DIR = "/app/data/uploads"  # persisted via bind mount

st.title("Logs AI — Admin")
st.caption("DB init and bulk file upload")

# --- 1) Schema init ---
st.subheader("1) Initialize / Re-apply DB schema")
if st.button("Apply schema.sql now"):
    rc = init_db_main()
    if rc == 0:
        st.success("Schema applied.")
    else:
        st.error(f"Schema apply failed (exit {rc}). Check container logs.")

# --- 2) Upload + auto-ingest ---
st.subheader("2) Upload parsed XLSX (or ZIP of XLSX)")
os.makedirs(UPLOAD_DIR, exist_ok=True)

uploaded = st.file_uploader(
    "Choose .xlsx or .zip",
    type=["xlsx", "zip"],
    accept_multiple_files=True,
    help="Each ZIP/XLSX is saved to /app/data/uploads, then ingested into Postgres."
)

auto_ingest = st.checkbox("Ingest to DB after upload", value=True)

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
                            if not info.filename.lower().endswith(".xlsx"):
                                continue
                            dest = os.path.join(UPLOAD_DIR, os.path.basename(info.filename))
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
            errors.append(f"{name}: {e}")

    if saved_paths:
        st.success(f"Saved {len(saved_paths)} file(s) to {UPLOAD_DIR}")
        st.code("\n".join(saved_paths))

        if auto_ingest:
            try:
                # import here to keep app_admin.py light
                from ingest.xlsx2db import ingest_folder
            except Exception as e:
                st.error(f"Ingest module not available: {e}")
                ingest_folder = None

            if ingest_folder:
                try:
                    with st.spinner("Ingesting to Postgres…"):
                        summary = ingest_folder()
                    st.success(
                        f"Ingest complete: files={summary.get('files',0)} "
                        f"rows={summary.get('rows',0)} dir={summary.get('dir','')}"
                    )
                except Exception as e:
                    st.error(f"Ingest failed: {e}")
                    st.code(traceback.format_exc())

    if errors:
        st.error("Some files failed:")
        st.code("\n".join(errors))