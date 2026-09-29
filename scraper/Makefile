# Load variables from parent .env
-include ../.env
export

DB_URL ?= postgres://$(PG_USER):$(PG_PASSWORD)@localhost:5432/$(PG_DB)?sslmode=disable
MIGRATE_BIN := go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

.PHONY: help
help:
	@echo "Available commands in Scraper:"
	@echo "  make run             - Run the scraper daemon"
	@echo "  make build           - Build executable binary to bin/scraper"
	@echo "  make tidy            - Run go mod tidy to clean up dependencies"
	@echo "  make vet             - Run go vet static analysis"
	@echo "  make migrate-create  - Create a new migration pair in migrations/"
	@echo "  make migrate-up      - Apply all pending database migrations"
	@echo "  make migrate-down    - Roll back all database migrations"
	@echo "  make clean           - Remove build artifacts and temporary files"

.PHONY: run
run:
	go run main.go

.PHONY: build
build:
	@mkdir -p bin
	go build -o bin/scraper main.go

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: migrate-create
migrate-create:
	@read -p "Enter migration name: " name; \
	$(MIGRATE_BIN) create -ext sql -dir migrations -seq $$name

.PHONY: migrate-up
migrate-up:
	$(MIGRATE_BIN) -path migrations -database "$(DB_URL)" up

.PHONY: migrate-down
migrate-down:
	$(MIGRATE_BIN) -path migrations -database "$(DB_URL)" down -all

.PHONY: clean
clean:
	rm -rf bin
	rm -rf logs/*.log
