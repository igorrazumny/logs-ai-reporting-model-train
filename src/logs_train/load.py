# File: src/logs_train/load.py
import os
import re
import json
import time
import base64
import requests
import duckdb
import yaml
from typing import Optional, Tuple, Iterator, List

# ======== Basic config ========
OLLAMA_HOST   = os.getenv("LLM_HOST", "http://ollama:11434")
# Default to faster 1B model for development (override via LLM_MODEL env)
LLM_MODEL     = os.getenv("LLM_MODEL", "llama3.2:1b-instruct-q4_K_M")
CSV_HEADER    = "User ID|ID|Subsequence ID|Message|Audit Time (UTC)|Action|Type|Label|Version"
REJECTS_PATH  = "outputs/pkm_rejects.log"

LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "60"))
RETRIES       = int(os.getenv("LLM_RETRIES", "2"))
BACKOFF_S     = float(os.getenv("LLM_BACKOFF_S", "1.0"))
MAX_RECORDS   = int(os.getenv("MAX_RECORDS", "10"))  # 0 = unlimited; default 10 for dev

# ======== YAML / helpers ========
def _read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _derive_actor(user: str, system_token: str, login_re: str, display_re: str) -> Tuple[Optional[str], Optional[str]]:
    """Derive (actor, actor_display) from 'Name (login)' or '(system)'."""
    if not user:
        return None, None
    user = user.strip()
    if user == system_token:
        return system_token, None
    m_login = re.search(login_re, user)
    m_disp  = re.search(display_re, user)
    login = m_login.group("login") if m_login else None
    disp  = m_disp.group("name").strip() if m_disp else None
    return (login or disp or user), disp

def _truncate(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"

# ======== Record iterator (balanced quotes only; no content changes) ========
def _iter_records(path: str) -> Iterator[str]:
    """
    Yield logical records where the full record is quoted and may contain pipes/newlines.
    Boundary rule: quotes must be balanced (we are not inside a quoted segment).
    Header rows are skipped (quoted or not, BOM-tolerant).
    """
    HEADER = CSV_HEADER
    buf = ""
    in_q = False
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if not line and not buf:
                continue
            buf = line if not buf else f"{buf}\n{line}"

            # Skip header (quoted or not; allow BOM)
            probe = buf.strip().lstrip("\ufeff").strip('"')
            if probe == HEADER:
                buf, in_q = "", False
                continue

            # Update quote state using the new line only
            i = 0
            while i < len(line):
                if line[i] == '"':
                    if in_q and i + 1 < len(line) and line[i + 1] == '"':
                        i += 2
                        continue
                    in_q = not in_q
                i += 1

            # Boundary = balanced quotes
            if not in_q and buf:
                yield buf
                buf = ""
        if buf and not in_q:
            yield buf

# ======== LLM call (message is base64 to preserve exact content) ========
_SYS_PROMPT = (
    "You are an expert log parsing engine. Return ONLY a single compact JSON object with exactly these keys:\n"
    '["user","id","subseq_id","message_b64","audit_utc","action","type","label","version"]\n'
    "- Do not include any other text. No markdown fences. No explanations.\n"
    "- 'message_b64' MUST be the base64-encoded form of the exact message text from the record; do not modify it.\n"
    "- If a value is unknown, use an empty string \"\".\n"
    "- Examples:\n"
    '  {\"user\":\"\",\"id\":\"1\",\"subseq_id\":\"1.23\",\"message_b64\":\"VXBkYXRlZCBNYXRlcmlhbHMgLi4u\",\"audit_utc\":\"2020-04-30 12:34:56\",\"action\":\"Change\",\"type\":\"Configuration\",\"label\":\"CONTAINER\",\"version\":\"NA\"}\n'
)

def _clean_llm_output(txt: str) -> str:
    """Strip common fences/backticks and leading 'json'."""
    clean = txt.strip()
    if clean.startswith("```"):
        parts = clean.split("```")
        candidates = [p for p in parts if "{" in p]
        clean = max(candidates, key=len) if candidates else clean
    clean = clean.lstrip().lstrip("json").lstrip()
    return clean

def _call_llm(record: str, timeout_s: int = LLM_TIMEOUT_S) -> Tuple[Optional[dict], str]:
    """Send one record to the local model. Return (parsed_json_or_None, raw_text)."""
    prompt = f"{_SYS_PROMPT}\nLOG RECORD:\n{record}"
    print(f"[llm] PROMPT: {_truncate(prompt)}")
    print(f"[llm] → sending at {time.strftime('%H:%M:%S')}")
    last_txt = ""
    for attempt in range(RETRIES + 1):
        try:
            r = requests.post(
                f"{OLLAMA_HOST}/api/generate",
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False, "options": {"temperature": 0.0}},
                timeout=timeout_s,
            )
            r.raise_for_status()
            txt = (r.json() or {}).get("response", "") if r.headers.get("content-type","").startswith("application/json") else r.text
            txt = (txt or "").strip()
            last_txt = txt
            print(f"[llm] ← received at {time.strftime('%H:%M:%S')} (len={len(txt)})")
            print(f"[llm] RAW: {_truncate(txt)}")
            clean = _clean_llm_output(txt)
            start, end = clean.find("{"), clean.rfind("}")
            if start != -1 and end != -1:
                js = clean[start:end+1]
                try:
                    return json.loads(js), txt
                except Exception as e:
                    print(f"[llm] JSON decode error: {e}")
                    # Try a very light sanitization (no content loss): remove unsanctioned fences/whitespace only
                    try:
                        return json.loads(js.strip()), txt
                    except Exception as e2:
                        print(f"[llm] still bad JSON: {e2}")
                        return None, txt
            return None, txt
        except Exception as e:
            wait = BACKOFF_S * (2 ** attempt)
            print(f"[llm] request error (attempt {attempt+1}/{RETRIES+1}): {e}, waiting {wait}s")
            time.sleep(wait)
    return None, last_txt

# ======== Loader (LLM-only, no clipping, base64 message) ========
def load_pkm_from_csv(
    csv_path: str,
    yaml_path: str = "adapters/pkm.yaml",
    db_path: str = "outputs/pkm.duckdb",
    truncate: bool = True,
    min_ok_ratio: float = 0.0,  # ignored; we never crash on ratio
) -> dict:
    print(f"[start] file={csv_path}")
    print(f"[start] db  ={db_path}")
    print(f"[hc] Ollama = {OLLAMA_HOST}")
    print(f"[hc] Model  = {LLM_MODEL}")

    # ensure outputs dir exists BEFORE opening DuckDB
    out_dir = os.path.dirname(os.path.abspath(db_path)) or "."
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(REJECTS_PATH)) or ".", exist_ok=True)

    try:
        requests.get(OLLAMA_HOST, timeout=5).raise_for_status()
    except Exception as e:
        raise RuntimeError(f"[hc] FATAL: cannot reach Ollama: {e}")

    cfg = _read_yaml(yaml_path)
    expected_keys = cfg["parse"]["fields"]
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

        for rec in _iter_records(csv_path):
            if MAX_RECORDS and total >= MAX_RECORDS:
                print(f"\n[stop] MAX_RECORDS={MAX_RECORDS} reached — stopping early.")
                break
            total += 1
            print("\n" + "="*80)
            print(f"[rec#{total}] RAW: {_truncate(rec.replace(chr(10),' ⏎ '))}")

            parsed, raw_txt = _call_llm(rec)
            # Validate minimal schema
            ok = parsed is not None and all(k in parsed for k in ["user","id","subseq_id","message_b64","audit_utc","action","type","label","version"])
            if not ok:
                print(f"[rec#{total}] → REJECT (missing keys)")
                rejected_lines.append(rec)
                rejects += 1
                continue

            # Decode base64 message (no clipping)
            msg_b64 = parsed.get("message_b64", "")
            try:
                msg_bytes = base64.b64decode(msg_b64.encode("utf-8"), validate=True)
                msg = msg_bytes.decode("utf-8")
            except Exception as e:
                print(f"[rec#{total}] base64 decode error: {e}")
                rejected_lines.append(rec)
                rejects += 1
                continue

            # message requirement
            if require_message and not msg.strip():
                print(f"[rec#{total}] → REJECT (empty message)")
                rejected_lines.append(rec)
                rejects += 1
                continue

            # derive actor/display
            actor, disp = _derive_actor(
                parsed.get("user",""),
                cfg["actors"]["system_token"],
                cfg["actors"]["login_regex"],
                cfg["actors"]["display_regex"],
            )

            vals = (
                parsed.get("audit_utc") or None,
                actor,
                disp,
                parsed.get("label") or None,
                parsed.get("action") or None,
                parsed.get("type") or None,
                parsed.get("id") or None,
                parsed.get("subseq_id") or None,
                parsed.get("version") or None,
                msg,  # exact message, decoded from base64
            )
            print(f"[rec#{total}] JSON OK (base64 message) → DB INSERT")
            con.execute("INSERT INTO logs_pkm VALUES (?,?,?,?,?,?,?,?,?,?)", vals)
            accepted += 1
            rowcount = con.execute("SELECT COUNT(*) FROM logs_pkm").fetchone()[0]
            print(f"[rec#{total}] DB INSERT OK rows_now={rowcount}")

        ratio = (accepted/total) if total else 0.0
        elapsed = time.time()-start
        print("\n" + "="*80)
        print(f"[done] inserted={accepted} seen={total} rejected={rejects} ratio={ratio:.1%} time={elapsed:.1f}s")

        # Write rejects report (no crash)
        with open(REJECTS_PATH,"w",encoding="utf-8") as fh:
            fh.write(f"Total:{total}\nAccepted:{accepted}\nRejected:{rejects}\n")
            if rejected_lines:
                fh.write("--- REJECTS (first 100) ---\n")
                for r in rejected_lines[:100]:
                    fh.write(r.replace("\n","\\n")+"\n")

        return {"inserted":accepted,"db":db_path,"seen":total,"rejected":rejects,"ok_ratio":ratio}
    finally:
        con.close()