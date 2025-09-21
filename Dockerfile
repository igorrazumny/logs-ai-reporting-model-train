# Project: logs-ai-reporting-model-train — File: Dockerfile
ARG BASE=cpu

FROM python:3.11-slim AS base_cpu
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04 AS base_cuda

FROM base_${BASE} AS app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

# System deps incl. libpq and psql client (for migrations/admin)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates build-essential pkg-config \
    libpq-dev postgresql-client && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# Code only — do not COPY empty data dirs; they are mounted at runtime.
COPY src/ ./src/
COPY docs/ ./docs/
COPY prompts/ ./prompts/
COPY README.md LICENSE.txt ./

# If you later add non-empty runtime resources under ./adapters/, COPY it back.
# COPY adapters/ ./adapters/

ENV PYTHONPATH=/app:/app/src

CMD ["python","-m","src.logs_train.cli","help"]