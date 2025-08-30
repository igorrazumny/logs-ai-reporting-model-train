# File: src/logs_train/load.py
import os
import re
import time
import requests
import duckdb
import yaml
from typing import Tuple, List

from logs_train.record_iter import iter_records
from logs_train.llm_client import call_llm_single as _call_llm

# ======== Basic config ========
OLLAMA_HOST   = os.getenv("LLM_HOST", "http://ollama:11434")
LLM_MODEL     = os.getenv("LLM_MODEL", "llama3.2:1b-instruct-q4_K_M")
CSV_HEADER    = "User ID|ID|Subsequence ID|Message|Audit Time (UTC)|Action|Type|Label|Version"
REJECTS_PATH  = "outputs/pkm_rejects.log"

LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "60"))
MAX_RECORDS   = int(os.getenv("MAX_RECORDS", "100"))  # 0 = unlimited

# ======== YAML / helpers ========
def _read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _derive_actor(user: str, system_token: str, login_re: str, display_re: str) -> Tuple[str, str | None]:
    if not user:
        return None, None
    user = user.strip()
    if user == system_token:
        return system_token, None
    m_login = re.search(login_re, user)
    m_disp  = re.search(display_re, user)
    login = m_login.group("login") if m_login else None
    disp  = m_disp.group("name").strip() if m_disp else None
    return login or disp or user, disp

def _truncate(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"

# ======== Loader (LLM-only, message as raw string) ========
def load_pkm_from_csv(
    csv_path: str,
    yaml_path: str = "adapters/pkm.yaml",
    db_path: str = "outputs/pkm.duckdb",
    truncate: bool = True,
) -> dict:
    print(f"[start] file={csv_path}")
    print(f"[start] db  ={db_path}")
    print(f"[hc] Ollama = {OLLAMA_HOST}")
    print(f"[hc] Model  = {LLM_MODEL}")

    # --- Guarantee directories exist (host + container) BEFORE connecting ---
    # Normalize db_path to absolute, then ensure its parent exists.
    db_path_abs = os.path.abspath(db_path)
    db_dir = os.path.dirname(db_path_abs) or "."
    os.makedirs(db_dir, exist_ok=True)
    # Keep your rejects log directory too
    rej_dir = os.path.dirname(os.path.abspath(REJECTS_PATH)) or "."
    os.makedirs(rej_dir, exist_ok=True)
    # Belt-and-suspenders: also ensure the typical container path
    os.makedirs("/app/outputs", exist_ok=True)

    # Healthcheck Ollama
    try:
        requests.get(OLLAMA_HOST, timeout=5).raise_for_status()
    except Exception as e:
        raise RuntimeError(f"[hc] cannot reach Ollama: {e}")

    cfg = _read_yaml(yaml_path)
    require_message = "message" in cfg.get("constraints", {}).get("require_fields", [])

    # --- Connect using the absolute db path we just ensured ---
    con = duckdb.connect(db_path_abs)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS logs_pkm("
            "ts TIMESTAMP, actor TEXT, actor_display TEXT, product TEXT, action TEXT, "
            "type TEXT, id TEXT, subseq_id TEXT, version TEXT, message TEXT)"
        )
        if truncate:
            con.execute("DELETE FROM logs_pkm")
            print("[db] cleared logs_pkm")

        total = accepted = rejected = 0
        rejected_lines: List[str] = []
        t0 = time.time()

        for rec in iter_records(csv_path):
            if MAX_RECORDS and total >= MAX_RECORDS:
                print(f"[stop] MAX_RECORDS={MAX_RECORDS} reached — stopping.")
                break
            total += 1
            print(f"\n[rec#{total}] RAW: {_truncate(rec)}")

            parsed, _ = _call_llm(rec)
            # Expect plain message (no base64)
            if parsed is None or not isinstance(parsed, dict):
                print(f"[rec#{total}] → REJECT (no/invalid JSON)")
                rejected += 1
                rejected_lines.append(rec)
                continue

            # Ensure all keys exist; fill unknown with ""
            KEYS = ["user","id","subseq_id","message","audit_utc","action","type","label","version"]
            data = {k: (parsed.get(k, "") if parsed.get(k, "") is not None else "") for k in KEYS}

            msg = (data["message"] or "").rstrip("\r")
            if require_message and not msg.strip():
                print(f"[rec#{total}] → REJECT (empty message)")
                rejected += 1
                rejected_lines.append(rec)
                continue

            # Normalize timestamp (bad values -> NULL)
            ts_raw = data["audit_utc"] or None
            ts_val = None
            if ts_raw:
                try:
                    ts_val = con.execute("SELECT TRY_CAST(? AS TIMESTAMP)", [ts_raw]).fetchone()[0]
                except Exception:
                    ts_val = None

            actor, disp = _derive_actor(
                data["user"],
                cfg["actors"]["system_token"],
                cfg["actors"]["login_regex"],
                cfg["actors"]["display_regex"],
            )

            vals = (
                ts_val,
                actor,
                disp,
                (data["label"] or None),
                (data["action"] or None),
                (data["type"] or None),
                (data["id"] or None),
                (data["subseq_id"] or None),
                (data["version"] or None),
                msg,
            )
            print(f"[rec#{total}] JSON OK → DB INSERT")
            con.execute("INSERT INTO logs_pkm VALUES (?,?,?,?,?,?,?,?,?,?)", vals)
            accepted += 1

        ratio = (accepted / total) if total else 0.0
        print(f"\n[done] inserted={accepted} seen={total} rejected={rejected} ratio={ratio:.1%} time={time.time()-t0:.1f}s")
        return {"inserted": accepted, "db": db_path_abs, "seen": total, "rejected": rejected, "ok_ratio": ratio}
    finally:
        con.close()