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
MAX_RECORDS   = int(os.getenv("MAX_RECORDS", "30"))  # 0 = unlimited

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

    # ensure outputs dir exists BEFORE opening DuckDB
    out_dir = os.path.dirname(os.path.abspath(db_path)) or "."
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(REJECTS_PATH)) or ".", exist_ok=True)
    # guard container path so DuckDB always has a directory
    os.makedirs("/app/outputs", exist_ok=True)

    try:
        requests.get(OLLAMA_HOST, timeout=5).raise_for_status()
    except Exception as e:
        raise RuntimeError(f"[hc] cannot reach Ollama: {e}")

    cfg = _read_yaml(yaml_path)
    require_message = "message" in cfg.get("constraints", {}).get("require_fields", [])

    con = duckdb.connect(db_path)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS logs_pkm("
            "ts TIMESTAMP, actor TEXT, actor_display TEXT, product TEXT, action TEXT, "
            "type TEXT, id TEXT, subseq_id TEXT, version TEXT, message TEXT)"
        )
        if truncate:
            con.execute("DELETE FROM logs_pkm")
            print("[db] cleared logs_pkm")

        total, accepted, rejects = 0, 0, 0
        rejected_lines: List[str] = []
        start = time.time()

        for rec in iter_records(csv_path):
            if MAX_RECORDS and total >= MAX_RECORDS:
                print(f"[stop] MAX_RECORDS={MAX_RECORDS} reached — stopping.")
                break
            total += 1
            print(f"\n[rec#{total}] RAW: {_truncate(rec)}")

            parsed, _ = _call_llm(rec)

            # Expect plain message (no base64)
            ok = parsed is not None and all(
                k in parsed for k in ["user","id","subseq_id","message","audit_utc","action","type","label","version"]
            )
            if not ok:
                print(f"[rec#{total}] → REJECT (missing keys)")
                rejected_lines.append(rec)
                rejects += 1
                continue

            msg = (parsed.get("message") or "").rstrip("\r")
            if require_message and not msg.strip():
                print(f"[rec#{total}] → REJECT (empty message)")
                rejected_lines.append(rec)
                rejects += 1
                continue

            # Normalize timestamp safely (let DuckDB validate)
            ts_raw = parsed.get("audit_utc") or None
            ts_val = None
            if ts_raw:
                try:
                    ts_val = con.execute("SELECT TRY_CAST(? AS TIMESTAMP)", [ts_raw]).fetchone()[0]
                    # If model glued extra fields into audit_utc, TRY_CAST returns None
                except Exception:
                    ts_val = None

            actor, disp = _derive_actor(
                parsed.get("user",""),
                cfg["actors"]["system_token"],
                cfg["actors"]["login_regex"],
                cfg["actors"]["display_regex"],
            )

            vals = (
                ts_val,                                # ts (None if unparsable)
                actor,                                 # actor
                disp,                                  # actor_display
                parsed.get("label") or None,          # product
                parsed.get("action") or None,         # action
                parsed.get("type") or None,           # type
                parsed.get("id") or None,             # id
                parsed.get("subseq_id") or None,      # subseq_id
                parsed.get("version") or None,        # version
                msg,                                  # message
            )
            print(f"[rec#{total}] JSON OK → DB INSERT")
            con.execute("INSERT INTO logs_pkm VALUES (?,?,?,?,?,?,?,?,?,?)", vals)
            accepted += 1

        ratio = (accepted/total) if total else 0.0
        elapsed = time.time()-start
        print(f"\n[done] inserted={accepted} seen={total} rejected={rejects} ratio={ratio:.1%} time={elapsed:.1f}s")
        return {"inserted":accepted,"db":db_path,"seen":total,"rejected":rejects,"ok_ratio":ratio}
    finally:
        con.close()