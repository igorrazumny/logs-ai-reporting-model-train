# File: src/logs_train/cli.py
import sys
from logs_train.load import load_pkm_from_excel

def main():
    if len(sys.argv) < 3 or sys.argv[1] != "load-pkm":
        print("Usage: python -m logs_train.cli load-pkm <excel_path> [<yaml_path>]")
        sys.exit(1)
    excel = sys.argv[2]
    yaml_path = sys.argv[3] if len(sys.argv) > 3 else "adapters/pkm.yaml"
    out = load_pkm_from_excel(excel, yaml_path=yaml_path)
    print(f"Loaded {out['inserted']} rows into {out['db']}")

if __name__ == "__main__":
    main()
