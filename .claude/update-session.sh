#!/bin/bash
# File: .claude/update-session.sh
# Purpose: Master automation script - run after EVERY turn
# Changelog (version auto-extracted from first entry below):
#   v2025-11-14.1 - Initial creation: costs update + CLAUDE_GENERAL.md sync

VERSION=$(grep -m 1 "^#   v" "$0" | sed -E 's/.*\s+(v[0-9]{4}-[0-9]{2}-[0-9]{2}\.\d+).*/\1/')

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <total_tokens>"
    echo "Example: $0 32021"
    exit 1
fi

TOTAL_TOKENS=$1

cd "$PROJECT_ROOT"

python3 "$SCRIPT_DIR/costs-tracker.py" "$TOTAL_TOKENS" > /dev/null 2>&1

bash "$SCRIPT_DIR/sync-general.sh" > /dev/null 2>&1

exit 0
