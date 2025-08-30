# File: src/logs_train/prompts.py

# Static prompt used for per-record JSON extraction.
# Keeping it here lets us tweak prompt text without touching the loader code.
PKM_SINGLE_JSON_PROMPT = (
    "You are an expert log parsing engine. Return ONLY a single compact JSON object with exactly these keys:\n"
    '["user","id","subseq_id","message_b64","audit_utc","action","type","label","version"]\n'
    "- Do not include any other text. No markdown fences. No explanations.\n"
    "- 'message_b64' MUST be the base64-encoded form of the exact message text from the record; do not modify it.\n"
    "- If a value is unknown, use an empty string \"\".\n"
    "- Examples:\n"
    '  {\"user\":\"\",\"id\":\"1\",\"subseq_id\":\"1.23\",\"message_b64\":\"VXBkYXRlZCBNYXRlcmlhbHMgLi4u\",\"audit_utc\":\"2020-04-30 12:34:56\",\"action\":\"Change\",\"type\":\"Configuration\",\"label\":\"CONTAINER\",\"version\":\"NA\"}\n'
)