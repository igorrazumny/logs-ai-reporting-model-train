# File: src/logs_train/runlog.py
import os
import time

def make_run_logger(app_name: str, base_dir: str = "outputs"):
    """
    Create outputs/<app>/<YYYYMMDD-HHMMSS>/loader_debug.log and return:
      (_log: callable, log_file: str)
    _log(line: str) appends a single line to the log file (never raises).
    """
    app = (app_name or "app").strip() or "app"
    run_ts = time.strftime("%Y%m%d-%H%M%S")
    log_dir = os.path.join(base_dir, app, run_ts)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "loader_debug.log")

    def _log(line: str) -> None:
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            # logging must never break the loader
            pass

    return _log, log_file