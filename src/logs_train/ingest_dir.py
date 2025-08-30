# File: src/logs_train/ingest_dir.py
import os
import sys
import time
import shutil
from typing import List

from logs_train.load import load_pkm_from_csv

INBOX     = os.getenv("INBOX", "data/inbox")
PROCESSED = os.getenv("PROCESSED", "data/processed")
FAILED    = os.getenv("FAILED", "data/failed")
YAML_PATH = os.getenv("ADAPTER_YAML", "adapters/pkm show-save DB files source data.yaml")
DB_PATH   = os.getenv("DB_PATH", "outputs/pkm show-save DB files source data.duckdb")

def _ensure_dirs():
    os.makedirs(INBOX, exist_ok=True)
    os.makedirs(PROCESSED, exist_ok=True)
    os.makedirs(FAILED, exist_ok=True)

def _list_csvs() -> List[str]:
    try:
        return sorted(
            os.path.join(INBOX, f)
            for f in os.listdir(INBOX)
            if f.lower().endswith(".csv") and os.path.isfile(os.path.join(INBOX, f))
        )
    except FileNotFoundError:
        return []

def main() -> int:
    _ensure_dirs()
    files = _list_csvs()
    if not files:
        print(f"[ingest] no CSV files in {INBOX}")
        return 0

    print(f"[ingest] found {len(files)} file(s) in {INBOX}")
    ok = 0
    bad = 0
    for i, path in enumerate(files, 1):
        print(f"\n[ingest] ({i}/{len(files)}) loading: {path}")
        try:
            # Always append, never truncate
            res = load_pkm_from_csv(
                csv_path=path,
                yaml_path=YAML_PATH,
                db_path=DB_PATH,
                truncate=False
            )
            print(f"[ingest] ok: {res}")
            # move to processed with timestamp suffix to avoid collisions
            base = os.path.basename(path)
            ts = time.strftime("%Y%m%d_%H%M%S")
            dst = os.path.join(PROCESSED, f"{ts}_{base}")
            shutil.move(path, dst)
            ok += 1
        except Exception as e:
            print(f"[ingest] FAILED: {e}")
            base = os.path.basename(path)
            ts = time.strftime("%Y%m%d_%H%M%S")
            dst = os.path.join(FAILED, f"{ts}_{base}")
            try:
                shutil.move(path, dst)
            except Exception as move_err:
                print(f"[ingest] could not move failed file: {move_err}")
            bad += 1

    print(f"\n[ingest] done: ok={ok} failed={bad} inbox={INBOX} processed={PROCESSED} failed={FAILED}")
    return 0 if bad == 0 else 1

if __name__ == "__main__":
    sys.exit(main())