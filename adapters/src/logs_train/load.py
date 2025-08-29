# File: src/logs_train/load.py
import os, glob, csv, hashlib, base64, re
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

def _read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _csv_split(line: str, delim: str="|", quotechar: str='"'):
    rdr = csv.reader([line], delimiter=delim, quotechar=quotechar)
    return next(rdr)

def _derive_actor(user: str, system_token: str, login_re: str, display_re: str):
    if not user:
        return None, None
    user = str(user).strip()
    if user == system_token:
        return system_token, None  # actor = "(system)", display = None
    m_login = re.search(login_re, user)
    m_disp  = re.search(display_re, user)
    login = m_login.group("login") if m_login else None
    disp  = m_disp.group("name").strip() if m_disp else None
    actor = login or disp or user
    return actor, disp

def load_pkm_from_excel(
    excel_path: str,
    yaml_path: str = "adapters/pkm.yaml",
    db_path: str   = "outputs/pkm.duckdb",
    truncate: bool = True
):
    """Load the April 2020-style PKM Excel into DuckDB logs_pkm with strict parsing."""
    cfg = _read_yaml(yaml_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # 1) Read Excel, single column with header that is the pipe header string
    df_x = pd.read_excel(excel_path, sheet_name=cfg["source"]["sheet"], dtype=str)
    if df_x.shape[1] != 1:
        raise RuntimeError(f"Expected a single-column Excel; got {df_x.shape}")
    colname = df_x.columns[0]
    expected = cfg["source"]["line_column_header"]
    if colname != expected:
        raise RuntimeError(f"Unexpected header.\nExpected: {expected}\nGot:      {colname}")

    # 2) Parse each cell as a pipe-delimited record (respecting quotes)
    fields = cfg["parse"]["fields"]
    rows = []
    for raw in df_x.iloc[:,0].dropna():
        parts = _csv_split(str(raw), cfg["parse"]["delimiter"], cfg["parse"]["quotechar"])
        if len(parts) != len(fields):
            # allow trailing NA-ish versions, but usually it should be exact
            raise RuntimeError(f"Bad field count in row: got {len(parts)} expected {len(fields)}\nRow: {raw}")
        rec = dict(zip(fields, parts))

        if any(k in cfg["constraints"]["require_fields"] and (rec.get(k) is None or str(rec.get(k)).strip()=="")
               for k in cfg["constraints"]["require_fields"]):
            raise RuntimeError(f"Missing required fields in row: {raw}")

        # derive actor + actor_display from 'user'
        actor, actor_disp = _derive_actor(
            rec.get("user",""),
            cfg["actors"]["system_token"],
            cfg["actors"]["login_regex"],
            cfg["actors"]["display_regex"],
        )

        # build target row
        rows.append({
            "ts"           : rec.get("audit_utc"),
            "actor"        : actor,
            "actor_display": actor_disp,
            "product"      : rec.get("label"),
            "action"       : rec.get("action"),
            "type"         : rec.get("type"),
            "id"           : rec.get("id"),
            "subseq_id"    : rec.get("subseq_id"),
            "version"      : rec.get("version"),
            "message"      : rec.get("message"),
        })

    # 3) Coerce to dataframe with correct dtypes
    df = pd.DataFrame(rows, columns=[
        "ts","actor","actor_display","product","action","type","id","subseq_id","version","message"
    ])
    # parse timestamps; keep NaT if unparsable
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    # 4) Create DuckDB and table, insert rows
    con = duckdb.connect(db_path)
    con.execute("""
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
    """)
    if truncate:
        con.execute("DELETE FROM logs_pkm")
    # fast insert via DuckDB's pandas integration
    con.execute("INSERT INTO logs_pkm SELECT * FROM df")
    con.close()
    return {"inserted": len(df), "db": db_path}

if __name__ == "__main__":
    # Example local run:
    # python -m logs_train.load path/to/2020-04\ Source\ Logs.xlsx
    import sys
    excel = sys.argv[1] if len(sys.argv) > 1 else "data/2020-04 Source Logs.xlsx"
    out = load_pkm_from_excel(excel)
    print(out)
