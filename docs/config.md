<!-- Project: logs-ai-reporting-model-train — File: docs/config.md -->

# Config — logs-ai-reporting-model-train

## 0) Scope & Ownership
- **Repository**: private / proprietary (see `LICENSE.txt`).
- **Owner**: Razum GmbH.
- **Purpose**: AI logs reporting solution with NL→SQL chat, execution against Postgres, and optional training/evaluation of per-application NL→SQL adapters (LoRA/QLoRA).

---

## 1) Data Handling & Schemas
- **Input policy**: All data must be anonymized on client premises before inclusion. No raw PII allowed.
- **Accepted formats**: Parquet (`.parquet`), JSON Lines (`.jsonl`).
- **Canonical staging table**:
  - `raw_logs(ts TIMESTAMP, source TEXT, actor TEXT, action TEXT, object TEXT, duration_ms BIGINT, project TEXT, payload_json JSONB)`
- **Provenance**: Anonymization salt and method remain under client custody and **must not** be stored in this repo or images.
- **Column conventions**:
  - Timestamps in UTC ISO 8601 at ingest; convert for UI at query time.
  - `payload_json` holds source-specific fields; never denormalize PII.

---

## 2) Runtime Environments
- **Local dev**: Python 3.11; `requirements.txt` for dependencies.
- **Containers**:
  - `Dockerfile` builds app image with `libpq-dev` and `postgresql-client`.
  - `docker-compose.yml` defines:
    - `app`: Streamlit UI + NL→SQL + reporting.
    - `db`: Postgres 15.
- **GPU (optional)**: CUDA base stage is present but not required for runtime; training scripts may use GPU when invoked explicitly.

---

## 3) Secrets & Environment Variables
- **No secrets in git**. Provide via environment, secrets manager, or `.env` (git-ignored).
- **Required at runtime**:
  - `GEMINI_STUDIO_API_KEY` — key for LLM calls.
- **Database connection (already set by compose for in-cluster use)**:
  - `DB_HOST=db`
  - `DB_PORT=5432`
  - `POSTGRES_USER=logsai_user`
  - `POSTGRES_PASSWORD=logsai_user_password`
  - `POSTGRES_DB=logsaidb`
- **Optional** (enable only if used):
  - `LLM_PROVIDER` (e.g., `gemini`)
  - `LLM_MODEL` (e.g., `gemini-1.5-pro`)
  - `ADMIN_TOKEN` — simple admin guard for UI admin routes.

---

## 4) Files, Prompts & Templates
- **Prompts**:
  - Default path: `prompts/prompt.txt` (included in the image).
  - Override option (ops): mount a custom file at `/app/prompts/prompt.txt` if client-specific prompt is required.
- **SFT guidance examples**:
  - `prompts/sql_sft_guidance_examples.jsonl` — non-secret examples for alignment/training data gen.
- **Do not store**: client secrets, salts, or real log samples with PII.

---

## 5) Persistence & Volumes
- **Named volumes** in `docker-compose.yml` (no host paths hardcoded):
  - `data` → mounted at `/app/data` — input data (uploads/staging).
  - `outputs` → mounted at `/app/outputs` — generated reports/logs/exports.
  - `pgdata` → mounted at `/var/lib/postgresql/data` — full Postgres cluster.
- **Rationale**: Portable by default; client DevOps may override to bind specific host paths via `docker-compose.override.yml`.

---

## 6) Database
- **Image**: `postgres:15`.
- **Port mapping**: host `5433` → container `5432` (see compose).
- **Initialization**:
  - App provides `src/db/schema.sql` and `src/db/init_db.py` for schema creation.
  - Migrations are executed via the app’s admin UI/CLI or `psql` using the installed `postgresql-client` inside the app container.
- **Backups** (logical):
  - Dump: `docker compose exec -T db pg_dump -U logsai_user -d logsaidb > backup.sql`
  - Restore: `docker compose exec -T db psql -U logsai_user -d logsaidb < backup.sql`
- **Operational notes**:
  - Never share `pgdata` between different PG major versions.
  - Back up `pgdata` or use logical dumps prior to upgrades.

---

## 7) LLM Integration
- **Adapter**: `src/llm/adapter.py` mediates provider calls.
- **Default**: Gemini via `GEMINI_STUDIO_API_KEY`.
- **Contract**:
  - **Phase A**: NL→SQL generation (no execution in LLM).
  - **DB phase**: Execute SQL server-side with guardrails and statement timeout.
  - **Phase B**: Natural-language summarization of result JSON (no SQL shown to user).
- **Prompt content**: Kept concise, enforce “no code fences, no SQL” in Phase B to avoid leaks.

---

## 8) NL→SQL Safety & Guardrails
- **Extraction**: `extract_sql()` strips code fences and isolates a single statement.
- **Validation**:
  - `looks_like_sql()` blocks accidental natural text from executing.
  - Limit to `SELECT` queries unless explicitly whitelisted for admin flows.
- **Execution policy**:
  - `exec_sql()` enforces statement timeout (server-side) and read-only user for chat path.
  - Log all executed SQL (sampled) via `log_sql_event()`; cap row previews in logs.
- **Auto-answer fallback**:
  - If Phase B fails or returns SQL, `auto_answer()` generates a terse answer from `cols, rows` with no SQL exposure.

---

## 9) UI & UX
- **Web UI**: Streamlit app (`src/ui/web/app.py`, `view_chat.py`, `chat_skin.py`, `examples.py`).
- **Chat behavior**:
  - Custom right-aligned user bubble; assistant renders GPT-style text.
  - Auto-scroll to bottom after send and after answer (JS anchor injection).
  - Spinner shows live elapsed using `fmt_elapsed()`.
- **History**:
  - Session-local history in `st.session_state["history"]`; compact recent-context block sent to Phase A/B.

---

## 10) Logging & Telemetry
- **Application logs**:
  - Chat/SQL logs under `/app/outputs/logs/` (persisted in `outputs` volume).
  - `log_sql_event()` captures user text, extracted SQL, error (if any), row counts, and elapsed time; redact sensitive values.
- **Access logs**:
  - Streamlit server logs are captured via container logs; forward to central logging if required at client site.
- **PII policy**:
  - Do not log raw payloads unless explicitly anonymized; prefer counts and sample-free summaries.

---

## 11) Training & Evaluation (optional)
- **Data generation**:
  - `src/training/data_gen/generate_sql_sft.py` creates Q→SQL pairs from anonymized snapshots.
  - Keep test splits (`sql_sft_examples_test.jsonl`) separate and execution-validated.
- **LoRA/QLoRA**:
  - Use PEFT; adapters and model cards reside under `models/` locally (git-ignored) and are shipped only in private images, not committed.
- **Evaluation**:
  - Exact-match and execution-match; hold-out by query pattern (date windows, group-bys, HAVING, nulls) to avoid leakage.

---

## 12) Security & Compliance
- **Access control**:
  - Private GitHub repo with least-privilege access.
  - Rotate `ADMIN_TOKEN` when giving temporary access.
- **Data constraints**:
  - No raw PII or reversible identifiers.
  - Timestamp shift + jitter + bucketing where applicable.
- **Publication**:
  - Benchmarks or results require written consent per `LICENSE.txt`.

---

## 13) Deployment & Operations
- **Compose**:
  - Named volumes: `data`, `outputs`, `pgdata`.
  - No host paths baked into compose; client DevOps may add overrides.
- **Rebuild/redeploy**:
  - Rebuild app image when `src/` or `prompts/` change.
- **Health**:
  - App depends on `db`; simple `restart: unless-stopped`.
- **Upgrades**:
  - For Postgres major upgrades, snapshot via dump + restore into a new cluster.
- **Disaster recovery**:
  - Ensure `pgdata` and `outputs` are part of backup schedule; include logical dumps for portability.

---

## 14) CI/CD (guidance)
- **Build**:
  - Use `.dockerignore` to keep context small (excludes `data/`, `models/`, `outputs/`, `backups/` and large files).
- **Scan**:
  - Add dependency and image scanning steps as required by client policy.
- **Registry**:
  - Tag images per release process (see Versioning); avoid embedding secrets.

---

## 15) Versioning & Releases
- **Runtime image tags**:
  - Use date-based tags `YYYY.MM.DD` with optional `-hotfixN`.
- **Model artifacts**:
  - When applicable, maintain `SHA256SUMS` for shipped adapter files.
- **Changelog**:
  - Maintain brief release notes in repo or registry release description.

---

## 16) Contacts
- **Technical owner**: Igor Razumny (Razum GmbH).
- **Legal/licensing**: see `LICENSE.txt`.