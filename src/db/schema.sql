-- File: logs-ai-reporting-model-train/db/schema.sql

-- Parsed log records (one row per normalized input line)
CREATE TABLE IF NOT EXISTS logs_pkm (
  -- base 9 fields (kept verbatim from parser)
  user_id           TEXT,
  id                TEXT,
  subseq_id         TEXT,
  message           TEXT,
  audit_time        TIMESTAMPTZ,
  action_raw        TEXT,
  type              TEXT,
  label             TEXT,
  version           TEXT,

  -- derived fields from parser
  recipe_id         TEXT,
  recipe_name       TEXT,
  material_name     TEXT,
  material_id       TEXT,
  name1             TEXT,
  name2             TEXT,
  username          TEXT,
  action_derived    TEXT,       -- first words of message (parser’s derived "action")

  -- session metadata
  session_start     TIMESTAMPTZ,
  session_end       TIMESTAMPTZ,
  session_duration  INTEGER      -- seconds; capped for auto check-ins
);

-- Query helpers (no defaults, just indexes)
CREATE INDEX IF NOT EXISTS idx_logs_pkm_audit_time      ON logs_pkm (audit_time);
CREATE INDEX IF NOT EXISTS idx_logs_pkm_username        ON logs_pkm (username);
CREATE INDEX IF NOT EXISTS idx_logs_pkm_recipe_id       ON logs_pkm (recipe_id);
CREATE INDEX IF NOT EXISTS idx_logs_pkm_material_id     ON logs_pkm (material_id);
CREATE INDEX IF NOT EXISTS idx_logs_pkm_action_derived  ON logs_pkm (action_derived);
CREATE INDEX IF NOT EXISTS idx_logs_pkm_session_start   ON logs_pkm (session_start);
CREATE INDEX IF NOT EXISTS idx_logs_pkm_session_end     ON logs_pkm (session_end);