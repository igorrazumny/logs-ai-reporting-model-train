# File: src/logs_train/load.py
import os
import re
import json
import duckdb
import pandas as pd
import requests
import yaml


# ---------- Config ----------
OLLAMA_HOST = os.getenv("LLM_HOST", "http://ollama:11434")
LLM_MODEL   = os.getenv("LLM_MODEL", "llama3.1:8b-instruct-q4_K_M")
REJECTS_PATH = "outputs/pkm_rejects.log"


# ---------- YAML / parsing helpers ----------
def _read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _derive_actor(user: str, system_token: str, login_re: str, display_re: str):
    """
    From 'Name Surname (login)' or '(system)' derive:
      actor = login if present, else display name, else raw string (or system token)
      actor_display = display name if present, else None for system
    """
    if not user:
        return None, None
    user = str(user).strip()
    if user == system_token:
        return system_token, None
    m_login = re.search(login_re, user)
    m_disp  = re.search(display_re, user)
    login = m_login.group("login") if m_login else None
    disp  = m_disp.group("name").strip() if m_disp else None
    actor = login or disp or user
    return actor, disp


def _split_pipe_ignoring_quotes(s: str) -> list[str]:
    """
    Split s on '|' but ignore pipes inside double-quoted segments (handles doubled quotes).
    """
    out, buf, in_q = [], [], False
    i, n = 0, len(s)
    while i < n:
        ch = s[i]
        if ch == '"':
            if in_q and i + 1 < n and s[i + 1] == '"':  # doubled quote -> literal "
                buf.append('"'); i += 2; continue
            in_q = not in_q
            i += 1
            continue
        if ch == '|' and not in_q:
            out.append(''.join(buf)); buf = []; i += 1; continue
        buf.append(ch); i += 1
    out.append(''.join(buf))
    return out


def _count_pipes_outside_quotes(s: str) -> int:
    in_q = False
    cnt = 0
    i, n = 0, len(s)
    while i < n:
        ch = s[i]
        if ch == '"':
            if in_q and i + 1 < n and s[i + 1] == '"':
                i += 2; continue
            in_q = not in_q; i += 1; continue
        if ch == '|' and not in_q:
            cnt += 1
        i += 1
    return cnt


def _record_complete(pipe_count_outside_quotes: int) -> bool:
    # 9 fields -> 8 pipes outside quotes
    return pipe_count_outside_quotes >= 8


def _strip_outer_quotes(s: str) -> str:
    """If the whole record is wrapped in double quotes, strip them and undouble inner quotes."""
    if not s or len(s) < 2:
        return s
    if s[0] == '"' and s[-1] == '"':
        return s[1:-1].replace('""', '"')
    return s


# ---------- LLM fallback ----------
def _llm_parse_line(line: str, timeout_s: int = 20) -> dict | None:
    """
    Ask local LLM (Ollama) to extract 9 fields. Returns dict or None if it can't parse.
    """
    sys_msg = (
        "Extract exactly these fields from the log line and return ONLY compact JSON:\n"
        '{"user","id","subseq_id","message","audit_utc","action","type","label","version"}\n'
        "- Do not add keys. Do not add text. Use empty string if unknown.\n"
        "- Preserve inner quotes in message."
    )
    prompt = f"{sys_msg}\n\nLOG LINE:\n{line}"
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
            timeout=timeout_s,
        )
        resp.raise_for_status()
        txt = resp.json().get("response", "").strip()
        # strip fenced code if present
        if txt.startswith("```"):
            parts = txt.split("```")
            if len(parts) >= 3:
                txt = parts[1]
            txt = txt.replace("json", "", 1).strip()
        return json.loads(txt)
    except Exception:
        return None


# ---------- Loader (with deterministic + LLM fallback) ----------
def load_pkm_from_csv(
    csv_path: str,
    yaml_path: str = "adapters/pkm.yaml",
    db_path: str = "outputs/pkm.duckdb",
    truncate: bool = True,
    min_ok_ratio: float = 0.70,   # fail only if below this ratio
):
    """
    Loader for single-column CSV where each logical record is pipe-delimited and the Message
    may contain '|' and newlines inside quotes. We accumulate lines until we see 8 pipes
    outside quotes (9 fields), then try deterministic parse; fallback to LLM if it fails.
    We never break on individual rows; we log rejects and continue. If overall success ratio
    < min_ok_ratio, we raise with a summary and path to rejects.
    """
    cfg = _read_yaml(yaml_path)
    fields = cfg["parse"]["fields"]  # expected field names in order
    header_joined = "User ID|ID|Subsequence ID|Message|Audit Time (UTC)|Action|Type|Label|Version"

    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)

    rows: list[dict] = []
    rejects: list[str] = []
    total = 0

    with open(csv_path, "r", encoding="utf-8", newline="") as fh:
        buf = ""
        for raw in fh:
            raw = raw.rstrip("\n")
            if not raw and not buf:
                continue

            # accumulate, preserving inner newlines for quoted message
            buf = raw if not buf else (buf + "\n" + raw)

            # skip header (quoted or not)
            probe = buf.strip()
            if probe == header_joined or (probe.startswith('"') and probe.endswith('"') and probe[1:-1] == header_joined):
                buf = ""
                continue

            # if record incomplete, continue accumulating
            if not _record_complete(_count_pipes_outside_quotes(buf)):
                continue

            total += 1
            record = _strip_outer_quotes(buf)

            # deterministic parse
            parts = _split_pipe_ignoring_quotes(record)
            rec: dict | None = None
            if len(parts) == len(fields):
                rec = dict(zip(fields, parts))
            else:
                # LLM fallback
                parsed = _llm_parse_line(record)
                if parsed and all(k in parsed for k in fields):
                    rec = parsed

            if rec is None:
                rejects.append(record)
                buf = ""
                continue

            # required fields (message)
            if "message" in cfg.get("constraints", {}).get("require_fields", []):
                if not str(rec.get("message", "")).strip():
                    rejects.append(record); buf = ""; continue

            actor, actor_disp = _derive_actor(
                rec.get("user", ""),
                cfg["actors"]["system_token"],
                cfg["actors"]["login_regex"],
                cfg["actors"]["display_regex"],
            )

            rows.append(
                {
                    "ts": rec.get("audit_utc"),
                    "actor": actor,
                    "actor_display": actor_disp,
                    "product": rec.get("label"),
                    "action": rec.get("action"),
                    "type": rec.get("type"),
                    "id": rec.get("id"),
                    "subseq_id": rec.get("subseq_id"),
                    "version": rec.get("version"),
                    "message": rec.get("message"),
                }
            )
            buf = ""

        # tail buffer
        if buf.strip():
            if _record_complete(_count_pipes_outside_quotes(buf)):
                total += 1
                record = _strip_outer_quotes(buf)
                parts = _split_pipe_ignoring_quotes(record)
                if len(parts) == len(fields):
                    rec = dict(zip(fields, parts))
                else:
                    rec = _llm_parse_line(record)
                if rec and ("message" not in cfg.get("constraints", {}).get("require_fields", []) or str(rec.get("message","")).strip()):
                    actor, actor_disp = _derive_actor(
                        rec.get("user", ""),
                        cfg["actors"]["system_token"],
                        cfg["actors"]["login_regex"],
                        cfg["actors"]["display_regex"],
                    )
                    rows.append(
                        {
                            "ts": rec.get("audit_utc"),
                            "actor": actor,
                            "actor_display": actor_disp,
                            "product": rec.get("label"),
                            "action": rec.get("action"),
                            "type": rec.get("type"),
                            "id": rec.get("id"),
                            "subseq_id": rec.get("subseq_id"),
                            "version": rec.get("version"),
                            "message": rec.get("message"),
                        }
                    )
                else:
                    rejects.append(record)

    # Write rejects report (non-blocking)
    if rejects:
        with open(REJECTS_PATH, "w", encoding="utf-8") as fh:
            fh.write(f"Total rows seen: {total}\n")
            fh.write(f"Accepted rows : {len(rows)}\n")
            fh.write(f"Rejected rows : {len(rejects)}\n")
            fh.write("--- REJECTS (escaped newlines as \\n) ---\n")
            for r in rejects:
                fh.write(r.replace("\n", "\\n") + "\n")

    # Threshold check (only fail if ratio below the bar)
    ok_ratio = (len(rows) / total) if total else 0.0
    if total and ok_ratio < min_ok_ratio:
        raise RuntimeError(
            f"Loaded {len(rows)}/{total} rows ({ok_ratio:.1%}) which is below threshold {min_ok_ratio:.0%}. "
            f"See {REJECTS_PATH} for details."
        )

    # Build DataFrame and write to DuckDB
    df = pd.DataFrame(
        rows,
        columns=[
            "ts",
            "actor",
            "actor_display",
            "product",
            "action",
            "type",
            "id",
            "subseq_id",
            "version",
            "message",
        ],
    )
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS logs_pkm(
            ts TIMESTAMP,
            actor TEXT,
            actor_display TEXT,
            product TEXT,
            action TEXT,
            type TEXT,
            id TEXT,
            subseq_id TEXT,
            version TEXT,
            message TEXT
        )
        """
    )
    if truncate:
        con.execute("DELETE FROM logs_pkm")
    if not df.empty:
        con.register("df_to_insert", df)
        con.execute("INSERT INTO logs_pkm SELECT * FROM df_to_insert")
    con.close()

    return {"inserted": int(len(df)), "db": db_path, "seen": int(total), "rejected": int(len(rejects)), "ok_ratio": ok_ratio}