# File: src/logs_train/cli.py
from logs_train.load import load_pkm_from_csv

def main():
    import sys
    if len(sys.argv) < 3 or sys.argv[1] != "load-pkm":
        print("Usage: python -m src.logs_train.cli load-pkm <csv_path> [<yaml_path>]")
        raise SystemExit(1)
    csv_path = sys.argv[2]
    yaml_path = sys.argv[3] if len(sys.argv) > 3 else "adapters/pkm.yaml"
    print(f"[cli] loading CSV: {csv_path}")
    print(f"[cli] using adapter: {yaml_path}")
    out = load_pkm_from_csv(csv_path, yaml_path=yaml_path)
    print(f"[cli] Loaded {out['inserted']} rows into {out['db']}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())