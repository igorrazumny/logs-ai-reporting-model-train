# File: Makefile

# ========== Local (venv) quick setup — optional if you prefer pure Docker ==========
.PHONY: venv deps
venv:
	python -m venv .venv

deps: venv
	. .venv/bin/activate && python -m pip install -U pip && \
	python -m pip install -r requirements.txt

# ========== Docker build/run ==========
.PHONY: build up down logs
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f app

# ========== LLM model management (Ollama) ==========
.PHONY: llm-pull-8b llm-pull-3b llm-list
llm-pull-8b:
	# Pull the 8B model
	docker compose up -d ollama
	docker exec -it ollama ollama pull llama3.1:8b-instruct-q4_K_M

llm-pull-3b:
	# Pull the faster 3B model (recommended for development on CPU)
	docker compose up -d ollama
	docker exec -it ollama ollama pull llama3.2:3b-instruct-q4_K_M

llm-list:
	docker exec -it ollama ollama list

# ========== Loader (runs inside Docker) ==========
# Usage: make load CSV="data/pkm/2020-04 Source Logs.csv"
.PHONY: load
load:
	test -n "$(CSV)" || (echo "Set CSV=<path> e.g. CSV='data/pkm/2020-04 Source Logs.csv'"; exit 2)
	docker compose run --rm app python -m src.logs_train.cli load-pkm "$(CSV)"

# ========== Streamlit UI (inside Docker) ==========
.PHONY: ui
ui:
	# Exposes Streamlit on http://localhost:8501
	docker compose run --rm -p 8501:8501 app python -m src.logs_train.ui_streamlit

# ========== First run convenience ==========
.PHONY: first-run
first-run: build up llm-pull-3b
	@echo "Now load your CSV with:"
	@echo "  make load CSV='data/pkm/2020-04 Source Logs.csv'"
	@echo "Then run the UI with:"
	@echo "  make ui"

# ========== Cold start (reset DB + limited run on 3B model) ==========
# Usage: make cold CSV="data/pkm/2020-04 Source Logs.csv" [N=20]
# - Shuts down containers
# - Removes DuckDB file
# - Starts containers
# - Loads using the 3B model with MAX_RECORDS=N (default 20)
N ?= 20
.PHONY: cold
cold:
	test -n "$(CSV)" || (echo "Set CSV=<path> e.g. CSV='data/pkm/2020-04 Source Logs.csv'"; exit 2)
	$(MAKE) down
	rm -f outputs/pkm.duckdb
	$(MAKE) up
	LLM_MODEL="llama3.2:3b-instruct-q4_K_M" MAX_RECORDS=$(N) $(MAKE) load CSV="$(CSV)"