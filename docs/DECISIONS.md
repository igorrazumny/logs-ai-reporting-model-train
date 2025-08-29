# DECISIONS

- 2025-08-28 — Base model: Llama-3.1-8B-Instruct for serving; 70B teacher for distillation.
- 2025-08-28 — Storage: anonymized Parquet + DuckDB for label execution checks.
- 2025-08-28 — Delivery: private GHCR image with LoRA adapter; no weights in git.
- 2025-08-28 — Anonymization: deterministic hashing + ts shift/jitter/minute bucket; salt stays client-side.
