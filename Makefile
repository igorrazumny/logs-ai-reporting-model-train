# ========== Local (venv) quick setup — optional if you prefer pure Docker ==========
.PHONY: venv deps
venv:
	python -m venv .venv

deps: venv
	. .venv/bin/activate && python -m pip install -U pip && \
	python -m pip install -r requirements.txt

# ========== Docker build/run ==========
.PHONY: build up down restart bounce reup logs
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

# Restart the stack (applies changes in .env and docker-compose.yml)
restart:
	docker compose down
	docker compose up -d

# Convenience aliases
bounce: restart
reup: restart

logs:
	docker compose logs -f app

# ========== LLM model management (Ollama) ==========
.PHONY: llm-pull-8b llm-pull-3b llm-list
llm-pull-8b:
	# Pull the 8B model
	docker compose up -d ollama
	docker exec -it ollama ollama pull llama3.1:8b-instruct-q4_K_M

llm-pull-3b:
	# Pull the 3B model (recommended for development on CPU)
	docker compose up -d ollama
	docker exec -it ollama ollama pull llama3.2:3b-instruct-q4_K_M

llm-list:
	docker exec -it ollama ollama list

# ========== Ensure host dirs exist ==========
.PHONY: init
init:
	mkdir -p outputs
	mkdir -p data

# ========== Loader (runs inside Docker) ==========
# Usage: make load CSV="data/pkm/2020-04 Source Logs.csv"
.PHONY: load
load: init
	test -n "$(CSV)" || (echo "Set CSV=<path> e.g. CSV='data/pkm/2020-04 Source Logs.csv'"; exit 2)
	# Guard inside the container too, so /app/outputs always exists even if the bind mount is empty/missing.
	docker compose run --rm -e MAX_RECORDS app sh -lc 'mkdir -p /app/outputs && python -m src.logs_train.cli load-pkm "$(CSV)"'

# ========== Streamlit UI (inside Docker) ==========
.PHONY: ui
ui: init
	# Exposes Streamlit on http://localhost:8501
	docker compose run --rm -p 8501:8501 app python -m src.logs_train.ui_streamlit

# ========== First run convenience ==========
.PHONY: first-run
first-run: build up llm-pull-3b
	@echo "Now load your CSV with:"
	@echo "  make load CSV='data/pkm/2020-04 Source Logs.csv'"
	@echo "Then run the UI with:"
	@echo "  make ui"

# ========== Cold start (reset DB + limited run; model comes from .env) ==========
# Usage: make cold CSV="data/pkm/2020-04 Source Logs.csv" [N=<count>]
# - Ensures outputs/ dir exists
# - Removes DuckDB file
# - Restarts containers (applies .env changes)
# - Loads using LOG_LLM_MODEL from .env with MAX_RECORDS=N; if N is not set -> MAX_RECORDS=0 (take all)
.PHONY: cold
cold:
	test -n "$(CSV)" || (echo "Set CSV=<path> e.g. CSV='data/pkm/2020-04 Source Logs.csv'"; exit 2)
	$(MAKE) init
	rm -f outputs/pkm.duckdb
	$(MAKE) restart
	@if [ -n "$(N)" ]; then \
		MAX_RECORDS="$(N)"; \
	else \
		MAX_RECORDS=0; \
	fi; \
	MAX_RECORDS="$$MAX_RECORDS" $(MAKE) load CSV="$(CSV)"

# ========== DB preview ==========
.PHONY: show
DB ?= outputs/pkm.duckdb
LIMIT ?= 0   # 0 = all rows
show:
	docker compose run --rm app python -m src.logs_train.show_db $(DB) $(LIMIT)