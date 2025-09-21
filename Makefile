# File: logs-ai-reporting-model-train/Makefile
# Minimal Makefile for logs-ai-reporting-model-train (Postgres + Admin UI)

# -------- Public targets --------

# Basic redeploy: rebuild app (keeps DB volume & uploads) and start services.
redeploy:
	docker compose build app
	$(MAKE) _start

# Clean redeploy: clear staged uploads, reset DB schema to empty (drop/recreate public), re-apply schema, then start.
redeploy-clean:
	$(MAKE) _clean-uploads
	$(MAKE) _db-reset-schema
	$(MAKE) _start

# Hard redeploy: remove containers & volumes, prune Docker cache, clear uploads, then start.
redeploy-hard:
	$(MAKE) _hard-reset
	$(MAKE) _clean-uploads
	$(MAKE) _start

# psql shell into the running DB.
psql:
	docker compose exec db psql -U logsai_user -d logsaidb

# Show row statistics per user table.
show-db-stats:
	docker compose exec db psql -U logsai_user -d logsaidb -c "\
		SELECT relname AS table, n_live_tup AS estimated_rows \
		FROM pg_stat_user_tables \
		ORDER BY relname;" && \
	docker compose exec db psql -U logsai_user -d logsaidb -c "\x" -c "SELECT * FROM logs_pkm WHERE type = 'recipe' AND session_duration > 0 LIMIT 50;"

show-recipe-sessions:
	docker compose exec db psql -U logsai_user -d logsaidb -c "\
		SELECT COUNT(*) AS recipe_sessions_gt0 \
		FROM logs_pkm \
		WHERE trim(lower(type))='recipe' AND COALESCE(session_duration,0)>0;" && \
	docker compose exec db psql -U logsai_user -d logsaidb -c "\x" -c "\
		SELECT * \
		FROM logs_pkm \
		WHERE trim(lower(type))='recipe' AND COALESCE(session_duration,0)>0 \
		ORDER BY audit_time DESC \
		LIMIT 100;"

# -------- Internal helpers (underscored) --------

# Start DB then app and print URLs.
_start:
	@echo "Starting Postgres and app..."
	docker compose up -d db
	docker compose up -d app
	@echo ""
	@echo "Chat:  http://localhost:8501/"
	@echo "Admin: http://localhost:8501/?admin=admin"

# Remove containers + named volumes and global unused data (images/networks), then bring DB up (app started by _start).
_hard-reset:
	docker compose down -v || true
	docker system prune -af --volumes || true
	docker compose up -d db

# Wipe staged XLSX in the bind-mounted uploads folder (host side).
_clean-uploads:
	rm -f data/uploads/*.xlsx 2>/dev/null || true
	@echo "Uploads cleared: data/uploads/"

# Reset DB to an empty state by dropping and recreating the public schema, then re-apply schema.sql.
_db-reset-schema:
	docker compose exec db psql -U logsai_user -d logsaidb -v ON_ERROR_STOP=1 -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO logsai_user;"
	$(MAKE) _db-apply-schema

# Apply db/schema.sql via the app container (uses db.init_db module).
_db-apply-schema:
	docker compose run --rm app python -m db.init_db

# (Optional diagnostics)
_db-ls:
	docker compose exec app sh -lc 'ls -lah /app/db || true'
_db-cat-schema:
	docker compose exec app sh -lc 'sed -n "1,120p" /app/db/schema.sql || echo missing'

.PHONY: redeploy redeploy-clean redeploy-hard psql _start _hard-reset _clean-uploads _db-reset-schema _db-apply-schema _db-ls _db-cat-schema