# File: src/logs_train/record_iter.py
from typing import Iterator

CSV_HEADER = "User ID|ID|Subsequence ID|Message|Audit Time (UTC)|Action|Type|Label|Version"

def iter_records(path: str) -> Iterator[str]:
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

            # Update quote state
            i = 0
            while i < len(line):
                if line[i] == '"':
                    if in_q and i + 1 < len(line) and line[i + 1] == '"':
                        i += 2
                        continue
                    in_q = not in_q
                i += 1

            if not in_q and buf:
                yield buf
                buf = ""
        if buf and not in_q:
            yield buf