# File: src/logs_train/prompts.py

# Schema-bound, few-shot prompt for PKM log parsing (no base64).
# Emphasizes: 9 fields, pipe-delimited, 4th field is the message (may contain pipes/newlines),
# and audit_utc MUST be the timestamp ONLY (no concatenated columns). All keys must exist.

PKM_SINGLE_JSON_PROMPT = (
    "You are an expert log parsing engine for PKM audit logs.\n"
    "\n"
    "LOG FORMAT\n"
    "- Each log record has EXACTLY 9 fields in this order:\n"
    "  [user, id, subseq_id, message, audit_utc, action, type, label, version]\n"
    "- Fields are pipe-delimited: |\n"
    "- Records are usually quoted; doubled quotes \"\" represent a literal quote.\n"
    "- The 4th field (between the 3rd and 5th pipes) is the MESSAGE. The message itself may contain pipes (|), quotes, and newlines. Do not split inside the message.\n"
    "- The 5th field is audit_utc and MUST be ONLY a timestamp like: 2020-04-30 15:33:55.541583 (no other fields concatenated).\n"
    "\n"
    "RESPONSE REQUIREMENTS\n"
    "- Return ONLY one compact JSON object with EXACTLY these keys in this order:\n"
    "  [\"user\",\"id\",\"subseq_id\",\"message\",\"audit_utc\",\"action\",\"type\",\"label\",\"version\"]\n"
    "- Every key MUST be present exactly once. If unknown, set to \"\".\n"
    "- The 'message' value MUST be copied VERBATIM from the message field (including any pipes and newlines) without modification.\n"
    "- No markdown fences. No commentary. JSON only.\n"
    "\n"
    "EXAMPLE (tricky case with pipes inside message and quoted label)\n"
    "INPUT RECORD:\n"
    "(system)|1|1.10|\"\"Updated Materials with values Material Name = APDL1 WCB EX GENEN. 5194| G102613/510853 |Material ID = 10147924\"\"|"
    "2020-04-30 15:33:36.984827|Change|Configuration|\"\"APDL1 WCB EX GENEN. 5194| G102613/510853\"\"|NA\n"
    "\n"
    "EXPECTED JSON:\n"
    "{"
      "\"user\":\"(system)\","
      "\"id\":\"1\","
      "\"subseq_id\":\"1.10\","
      "\"message\":\"Updated Materials with values Material Name = APDL1 WCB EX GENEN. 5194| G102613/510853 |Material ID = 10147924\","
      "\"audit_utc\":\"2020-04-30 15:33:36.984827\","
      "\"action\":\"Change\","
      "\"type\":\"Configuration\","
      "\"label\":\"APDL1 WCB EX GENEN. 5194| G102613/510853\","
      "\"version\":\"NA\""
    "}\n"
)