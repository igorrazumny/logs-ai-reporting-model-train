# Dockerifle

ARG BASE=cpu

# ---- Base selector ----
FROM python:3.11-slim AS base_cpu
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04 AS base_cuda

# ---- Runtime base ----
FROM base_${BASE} AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

# OS deps (both images)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# Project code (no data/models thanks to .dockerignore)
COPY scripts/ ./scripts/
COPY docs/ ./docs/
COPY README.md LICENSE.txt ./

# Default entry (overridable)
ENTRYPOINT ["python", "-m"]
CMD ["scripts.help"]
