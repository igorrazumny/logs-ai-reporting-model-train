# File: logs-ai-reporting-model-train/Makefile
# Minimal skeleton for logs-ai-reporting-model-train (Postgres migration prep)

.PHONY: psql serve build up down restart logs init ingest show show-save nlq hard-reset redeploy

serve:
	@echo "Starting Postgres and app..."
	docker compose up -d db
	docker compose up -d app
	@echo ""
	@echo "Chat:  http://localhost:8501/"
	@echo "Admin: http://localhost:8501/?admin=admin"

# --- Docker lifecycle ---
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d

# Wipe containers + volumes + unused images/networks; then bring stack back up
hard-reset:
	docker compose down -v || true
	docker system prune -af --volumes || true
	docker compose up -d

# Full rebuild + deploy
redeploy:
	docker compose build app
	$(MAKE) serve

logs:
	docker compose logs -f app

# Open interactive psql shell in the running db container
psql:
	docker compose exec db psql -U logsai_user -d logsaidb