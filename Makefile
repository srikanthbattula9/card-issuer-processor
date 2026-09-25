.PHONY: up migrate test
up:
	docker compose up -d
migrate:
	psql postgresql://postgres:postgres@localhost:5432/cards -f db/001_schema.sql
test:
	DATABASE_URL=postgresql://postgres:postgres@localhost:5432/cards pytest -q
