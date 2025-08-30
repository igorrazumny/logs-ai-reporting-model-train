# File: Makefile
# Minimal, no-legacy Makefile for logs-ai-reporting-model-train

# ========== Global config variables ==========
# You can override any of these on the command line, e.g.:
#   make ingest INBOX=data/inbox N=100
DB_PATH       ?= outputs/pkm.duckdb
DB            ?= $(DB_PATH)

# Record limits
N             ?= 20      # for cold runs (used to cap ingest); set N=0 to take all
LIMIT         ?= 0       # for show/show-save (0 = all)

# Ingest directories
INBOX         ?= data/inbox
PROCESSED     ?= data/processed
FAILED        ?= data/failed

# Adapter definition
ADAPTER_YAML  ?= adapters/pkm.yaml

# ========== Local (venv) quick setup — optional if you prefer pure Docker ==========
.PHONY: venv deps
venv:
	python -m venv .venv

deps: venv
	. .venv/bin/activate && python -m pip install -U pip && \
	python -m pip install -r requirements.txt

# ========== Docker build/run ==========
.PHONY: build up down restart logs
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

logs:
	docker compose logs -f app

# ========== LLM utility (optional) ==========
.PHONY: llm-list
llm-list:
	docker exec -it ollama ollama list

# ========== Ensure host dirs exist ==========
.PHONY: init
init:
	mkdir -p outputs
	mkdir -p data
	mkdir -p backups
	mkdir -p $(INBOX) $(PROCESSED) $(FAILED)

# ========== Bulk ingest from an inbox (preferred path) ==========
.PHONY: ingest
ingest: init
	# Process CSVs from inbox, optionally capped by MAX_RECORDS=N
	docker compose run --rm \
		-e INBOX=$(INBOX) \
		-e PROCESSED=$(PROCESSED) \
		-e FAILED=$(FAILED) \
		-e ADAPTER_YAML=$(ADAPTER_YAML) \
		-e DB_PATH=$(DB_PATH) \
		$(if $(N),-e MAX_RECORDS=$(N),) \
		app python -m src.logs_train.ingest_dir

# ========== Ad-hoc single-file load (keep for testing only) ==========
# Usage: make load CSV="data/inbox/some.csv"
.PHONY: load
load: init
	test -n "$(CSV)" || (echo "Set CSV=<path> e.g. CSV='data/inbox/2020-04 Source Logs Part 1.csv'"; exit 2)
	docker compose run --rm -e MAX_RECORDS app sh -lc 'mkdir -p /app/outputs && python -m src.logs_train.cli load-pkm "$(CSV)"'

# ========== DB preview ==========
.PHONY: show show-save
show:
	docker compose run --rm \
		-e ADAPTER_YAML=$(ADAPTER_YAML) \
		app python -m src.logs_train.show_db $(DB) $(LIMIT)

show-save:
	@mkdir -p outputs/pkm
	@ts=$$(date +%Y%m%d_%H%M%S_%3N); \
	out="outputs/pkm/show_$${ts}_limit$(LIMIT).jsonl"; \
	echo "[show-save] writing to $$out"; \
	docker compose run --rm \
		-e ADAPTER_YAML=$(ADAPTER_YAML) \
		app python -m src.logs_train.show_db "$(DB)" "$(LIMIT)" > "$$out"; \
	echo "[show-save] done: $$out"

# ========== Truncate PKM table (manual clean; we do NOT drop the DB file) ==========
.PHONY: truncate-pkm
truncate-pkm: init
	docker compose run --rm app duckdb $(DB_PATH) "DELETE FROM logs_pkm;"

# ========== Backup DB ==========
# Creates a timestamped copy AND updates backups/latest.duckdb
.PHONY: backup-db
backup-db: init
	@ts=$$(date +%Y%m%d_%H%M%S); \
	dst_ts="backups/pkm_$${ts}.duckdb"; \
	echo "[backup-db] saving $(DB_PATH) -> $$dst_ts"; \
	cp -f $(DB_PATH) $$dst_ts; \
	echo "[backup-db] updating backups/latest.duckdb"; \
	cp -f $$dst_ts backups/latest.duckdb; \
	echo "[backup-db] done: $$dst_ts and backups/latest.duckdb"

# ========== Restore DB from the latest backup ==========
.PHONY: restore-db
restore-db: init
	@if [ ! -f backups/latest.duckdb ]; then \
		echo "[restore-db] ERROR: backups/latest.duckdb not found. Run 'make backup-db' first."; exit 2; \
	fi
	@echo "[restore-db] restoring backups/latest.duckdb -> $(DB_PATH)"
	@cp -f backups/latest.duckdb $(DB_PATH)
	@echo "[restore-db] done."

# ========== Cold start (reset DB and ingest from INBOX) ==========
# Usage: make cold [N=<count>]
# - Ensures dirs exist
# - Removes DuckDB file
# - Restarts containers (applies .env changes)
# - Runs ingest with MAX_RECORDS=N (0 or unset = take all)
.PHONY: cold
cold:
	$(MAKE) init
	rm -f $(DB_PATH)
	$(MAKE) restart
	$(MAKE) ingest N=$(N)