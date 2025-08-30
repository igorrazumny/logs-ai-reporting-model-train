# File: src/logs_train/prompts.py

# Static prompt used for per-record JSON extraction.
PKM_SINGLE_JSON_PROMPT = (
    "You are an expert log parsing engine. Return ONLY a single compact JSON object with exactly these keys:\n"
    '["user","id","subseq_id","message","audit_utc","action","type","label","version"]\n'
    "- Do not include any other text. No markdown fences. No explanations.\n"
    "- The 'message' field MUST contain the exact text of the log message without modification.\n"
    "- If a value is unknown, use an empty string \"\".\n"
)