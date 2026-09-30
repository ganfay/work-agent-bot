# Load variables from parent .env
-include ../.env
export

.PHONY: help
help:
	@echo "Available commands in TG Bot:"
	@echo "  make run        - Run the Telegram Bot service"
	@echo "  make build      - Build binary to bin/tg_bot"
	@echo "  make tidy       - Run go mod tidy"
	@echo "  make vet        - Run go vet static analysis"
	@echo "  make clean      - Remove build artifacts and logs"

.PHONY: run
run:
	go run main.go

.PHONY: build
build:
	@mkdir -p bin
	go build -o bin/tg_bot main.go

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: clean
clean:
	rm -rf bin
	rm -rf logs/*.log
