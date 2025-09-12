# Config — logs-ai-reporting-model-train

## 0) Scope & Ownership
- Repo: **private / proprietary** (see LICENSE.txt). 
- Owner: Razum GmbH (brand: RazumAI).
- Purpose: training per-application NL→SQL models (LoRA/QLoRA) + evaluation.

## 1) Data Handling
- Input data **must be anonymized on client premises** before inclusion here.
- Allowed formats: Parquet (.parquet), JSONL (.jsonl). No raw PII.
- Canonical table: `raw_logs(ts, source, actor, action, object, duration_ms, project, payload_json)`.
- Provenance: record anonymization salt custody at client site (never stored here).

## 2) Environments
- Local dev: Python 3.11, CUDA (if GPU), `requirements.txt`.
- Training GPUs: A100/H100/4090 (LoRA), CPU only for label generation.
- Base model: `meta-llama/Meta-Llama-3.1-8B-Instruct` (primary), optional teacher `...-70B-Instruct`.

## 3) Secrets & Paths
- No secrets in repo. Use `.env` (gitignored).
- Local folder policy:
  - `data/`  – anonymized inputs (ignored).
  - `models/<client>/<app>/vYYYY.MM.DD/` – adapters & eval (ignored).
  - `outputs/` – logs/reports (ignored).

## 4) Training Policy
- Method: LoRA/QLoRA (PEFT). Default: r=16, α=16, dropout=0.05, epochs=1–5.
- Dataset: programmatically generated Q→SQL pairs (execution-validated).
- Eval: exact-match + execution-match; hold-out by pattern, not random.

## 5) Label Generation
- Source: stratified sample from anonymized `raw_logs`.
- Templates: filters, group-bys, date windows, HAVING, null/edge cases.
- Execution filter: keep only SQL that executes successfully on DuckDB snapshot.

## 6) Model Artifacts
- Deliverable: LoRA adapter (`adapter.safetensors`), `MODEL_CARD.md`, `eval.json`.
- Shipping: included in **private** Docker image, not committed to git.

## 7) Versioning
- Model tag: `<client>-<app>-YYYY.MM.DD[-hotfixN]`.
- Image tag mirrors model tag. Maintain `SHA256SUMS` for artifacts.

## 8) Security & Compliance
- No raw PII leaves client site. Deterministic hashing + timestamp shift+jitter+bucketing.
- Benchmarks/results may not be published without written consent (see LICENSE).
- Access control: GitHub private repo; grant temporary read access only when needed.

## 9) Federated Readiness
- Per-site LoRA adapters compatible with future FedAvg/FedProx aggregation.
- Site-local trainer CLI kept here; only adapters and eval JSONs leave sites.

## 10) Contacts
- Tech/Owner: Igor Razumny (Razum GmbH / RazumAI)
- Legal/License: see LICENSE.txt
