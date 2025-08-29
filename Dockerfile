# File: Dockerfile
ARG BASE=cpu

FROM python:3.11-slim AS base_cpu
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04 AS base_cuda

FROM base_${BASE} AS app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates build-essential pkg-config && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# project code
COPY src/ ./src/
COPY adapters/ ./adapters/
COPY docs/ ./docs/
COPY README.md LICENSE.txt ./

# src/ layout
ENV PYTHONPATH=/app/src

# default command (so compose/run can override cleanly)
CMD ["python","-m","src.logs_train.cli","help"]