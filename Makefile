.PHONY: up migrate test
up:
	docker compose up -d
migrate:
	docker compose exec -T postgres psql -U postgres -d cards -f /docker-entrypoint-initdb.d/001_schema.sql
test:
	EVENTS_DISABLED=1 DATABASE_URL=postgresql://postgres:postgres@localhost:5433/cards pytest -q