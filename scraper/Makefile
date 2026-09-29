include ../.env
export

DB_URL=postgres://$(PG_USER):$(PG_PASSWORD)@localhost:5432/$(PG_DB)?sslmode=disable

.PHONY: run
run:
	go run main.go

.PHONY: migrate-create
migrate-create:
	@read -p "Enter migration name: " name; \
	go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest create -ext sql -dir migrations -seq $$name

.PHONY: migrate-up
migrate-up:
	go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest -path migrations -database "$(DB_URL)" up

.PHONY: migrate-down
migrate-down:
	go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest -path migrations -database "$(DB_URL)" down -all
