# File: src/logs_train/load.py
import os
import time
import requests
import duckdb
import yaml
from typing import List, Tuple

from logs_train.record_iter import iter_records
from logs_train.llm_client import call_llm_single as _call_llm

# ======== Basic config ========
OLLAMA_HOST   = os.getenv("LOG_LLM_HOST")
LLM_MODEL     = os.getenv("LOG_LLM_MODEL")
# Global rejects log (rolling across runs)
REJECTS_PATH  = "outputs/pkm_rejects.log"
# Per-run rejects log (new file created each run, timestamped)
REJECTS_DIR   = "outputs/pkm show-save DB files source data"
REJECTS_FILE  = None  # set at runtime

LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "60"))
MAX_RECORDS   = int(os.getenv("MAX_RECORDS", "0"))  # 0 = unlimited

LLM_MAX_RETRIES   = int(os.getenv("LLM_MAX_RETRIES", "10"))   # retry each record up to N times
LLM_RETRY_SLEEP_S = float(os.getenv("LLM_RETRY_SLEEP_S", "0.5"))  # small pause between retries

# ======== YAML / helpers ========
def _read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _truncate(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"

def _call_llm_with_retries(record: str) -> Tuple[dict | None, str, int]:
    """
    Try the LLM up to LLM_MAX_RETRIES times.
    Returns: (parsed_json_or_none, last_raw_text, attempts_used)
    """
    last_txt = ""
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        parsed, txt = _call_llm(record)
        last_txt = txt
        if isinstance(parsed, dict) and parsed:
            if attempt > 1:
                print(f"[llm] success after retry #{attempt}")
            return parsed, last_txt, attempt
        if attempt < LLM_MAX_RETRIES:
            print(f"[llm] retry {attempt}/{LLM_MAX_RETRIES} failed — retrying in {LLM_RETRY_SLEEP_S}s…")
            time.sleep(LLM_RETRY_SLEEP_S)
    # exhausted retries
    print(f"[llm] all {LLM_MAX_RETRIES} attempts failed")
    return None, last_txt, LLM_MAX_RETRIES

# ======== Loader (driven 1:1 by YAML.fields) ========
def load_pkm_from_csv(
    csv_path: str,
    yaml_path: str = "adapters/pkm show-save DB files source data.yaml",
    db_path: str = "outputs/pkm show-save DB files source data.duckdb",
    truncate: bool = True,
) -> dict:
    print(f"[start] file={csv_path}")
    print(f"[start] db  ={db_path}")
    print(f"[hc] Ollama = {OLLAMA_HOST}")
    print(f"[hc] Model  = {LLM_MODEL}")

    # --- Guarantee directories exist (host + container) BEFORE connecting ---
    db_path_abs = os.path.abspath(db_path)
    db_dir = os.path.dirname(db_path_abs) or "."
    os.makedirs(db_dir, exist_ok=True)
    rej_dir = os.path.dirname(os.path.abspath(REJECTS_PATH)) or "."
    os.makedirs(rej_dir, exist_ok=True)
    os.makedirs("/app/outputs", exist_ok=True)

    # Healthcheck Ollama
    try:
        requests.get(OLLAMA_HOST, timeout=5).raise_for_status()
    except Exception as e:
        raise RuntimeError(f"[hc] cannot reach Ollama: {e}")

    # Load YAML config
    cfg = _read_yaml(yaml_path) or {}

    # Require explicit app name; no defaults
    if "app" not in cfg or not str(cfg["app"]).strip():
        raise RuntimeError(f"[cfg] missing required 'app' in {yaml_path}")
    app_name = str(cfg["app"]).strip()
    table = f"logs_{app_name}"

    # --- Setup rejects logging (now that app_name is known) ---
    os.makedirs(REJECTS_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_rejects_path = os.path.join(REJECTS_DIR, f"rejects_{app_name}_{ts}.log")
    global REJECTS_FILE
    REJECTS_FILE = run_rejects_path
    header = f"--- run start {time.strftime('%Y-%m-%d %H:%M:%S')} file={csv_path} model={LLM_MODEL} ---\n"
    for path in (REJECTS_PATH, REJECTS_FILE):
        with open(path, "a", encoding="utf-8") as f:
            f.write(header)

    # Fields drive everything 1:1 from YAML
    SRC_FIELDS = cfg.get("fields") or []
    if not SRC_FIELDS:
        raise RuntimeError(f"[cfg] 'fields' is required in {yaml_path}")

    # Optional constraint flag
    require_message = "message" in cfg.get("constraints", {}).get("require_fields", [])

    con = duckdb.connect(db_path_abs)
    try:
        # Create table with columns exactly as in YAML.fields (all TEXT for raw landing)
        cols_sql = ", ".join([f'"{c}" TEXT' for c in SRC_FIELDS])
        con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
        if truncate:
            con.execute(f'DELETE FROM "{table}"')
            print(f'[db] cleared {table}')

        total = accepted = rejected = 0
        rejected_lines: List[str] = []
        t0 = time.time()

        for rec in iter_records(csv_path):
            if MAX_RECORDS and total >= MAX_RECORDS:
                print(f"[stop] MAX_RECORDS={MAX_RECORDS} reached — stopping.")
                break
            total += 1
            print(f"\n[rec#{total}] RAW: {_truncate(rec)}")

            parsed, _raw_txt, attempts = _call_llm_with_retries(rec)
            # Expect plain message (no base64)
            if parsed is None or not isinstance(parsed, dict):
                print(f"[rec#{total}] → REJECT (no/invalid JSON after {attempts} attempt(s))")
                rejected += 1
                rejected_lines.append(rec)
                for path in (REJECTS_PATH, REJECTS_FILE):
                    with open(path, "a", encoding="utf-8") as f:
                        f.write(rec + "\n")
                continue

            # Ensure all YAML fields exist in the parsed output; fill missing with ""
            data = {k: (parsed.get(k, "") if parsed.get(k, "") is not None else "") for k in SRC_FIELDS}

            # Optional: enforce non-empty message
            if "message" in data:
                msg_text = (data["message"] or "").rstrip("\r")
                if require_message and not msg_text.strip():
                    print(f"[rec#{total}] → REJECT (empty message)")
                    rejected += 1
                    rejected_lines.append(rec)
                    for path in (REJECTS_PATH, REJECTS_FILE):
                        with open(path, "a", encoding="utf-8") as f:
                            f.write(rec + "\n")
                    continue
                data["message"] = msg_text

            # Build row values in the exact YAML order
            row_vals = [data[k] for k in SRC_FIELDS]

            # Insert into the exact YAML fields
            cols_quoted = ",".join([f'"{c}"' for c in SRC_FIELDS])
            placeholders = ",".join(["?"] * len(row_vals))
            con.execute(f'INSERT INTO "{table}" ({cols_quoted}) VALUES ({placeholders})', row_vals)
            print(f"[rec#{total}] JSON OK → DB INSERT")
            accepted += 1

        ratio = (accepted / total) if total else 0.0
        print(f"\n[done] inserted={accepted} seen={total} rejected={rejected} ratio={ratio:.1%} time={time.time()-t0:.1f}s")
        return {"inserted": accepted, "db": db_path_abs, "seen": total, "rejected": rejected, "ok_ratio": ratio}
    finally:
        con.close()