# File: Makefile

# --- Local (venv) quick setup, optional if you prefer pure Docker ---
.PHONY: venv deps
venv:
	python -m venv .venv

deps: venv
	. .venv/bin/activate && python -m pip install -U pip && \
	python -m pip install -r requirements.txt

# --- Docker build/run ---
.PHONY: build up down logs
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f app

# --- LLM model management (Ollama) ---
.PHONY: llm-pull llm-list
llm-pull:
	# Pull the 8B model once; adjust tag if you prefer a different quant
	docker compose up -d ollama
	docker exec -it ollama ollama pull llama3.1:8b-instruct-q4_K_M

llm-list:
	docker exec -it ollama ollama list

# --- Loader (runs *inside* Docker) ---
# Usage: make load CSV="data/pkm/2020-04 Source Logs.csv"
.PHONY: load
load:
	test -n "$(CSV)" || (echo "Set CSV=<path> e.g. CSV='data/pkm/2020-04 Source Logs.csv'"; exit 2)
	docker compose run --rm app python -m src.logs_train.cli load-pkm "$(CSV)"

# --- Streamlit UI (inside Docker) ---
.PHONY: ui
ui:
	# Exposes Streamlit on http://localhost:8501
	docker compose run --rm -p 8501:8501 app python -m src.logs_train.ui_streamlit

# --- One-shot: everything for first run (build + ollama + model) ---
.PHONY: first-run
first-run: build up llm-pull
	@echo "Now load your CSV with:  make load CSV='data/pkm/2020-04 Source Logs.csv'"
	@echo "Then run the UI with:    make ui"