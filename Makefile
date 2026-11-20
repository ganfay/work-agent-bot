# Root Master Makefile for Freelance Agent Monorepo
-include .env
export

DOCKER_COMPOSE := docker compose

.PHONY: help
help:
	@echo "======================================================================"
	@echo "           FREELANCE AGENT - MASTER CONTROL CENTER                    "
	@echo "======================================================================"
	@echo "  make up             - Build and start all 5 services in background"
	@echo "  make run-once       - Run full stack in RUN_ONCE=true batch mode, then tear down (0 idle RAM)"
	@echo "  make down           - Stop all running containers"
	@echo "  make restart        - Restart all containers"
	@echo "  make ps             - View status of all system containers"
	@echo "  make logs           - Stream combined logs from all services"
	@echo "  make logs-ai        - Stream logs exclusively from Python AI Worker"
	@echo "  make logs-bot       - Stream logs exclusively from Go Telegram Bot"
	@echo "  make logs-scraper   - Stream logs exclusively from Go Djinni Scraper"
	@echo "  make migrate-up     - Run database migrations on PostgreSQL"
	@echo "  make migrate-down   - Roll back database migrations"
	@echo "  make index          - Manually re-index pgvector embeddings"
	@echo "  make test           - Run all Go (-race) and Python unit tests"
	@echo "  make clean          - Stop containers and purge docker volumes"
	@echo "======================================================================"

.PHONY: up
up:
	$(DOCKER_COMPOSE) up -d --build

.PHONY: run-once
run-once:
	$(DOCKER_COMPOSE) up -d db rabbitmq
	@status=0; RUN_ONCE=true RESTART_POLICY=no $(DOCKER_COMPOSE) up --build scraper ai_worker tg_bot || status=$$?; \
	$(DOCKER_COMPOSE) down; \
	exit $$status

.PHONY: down
down:
	$(DOCKER_COMPOSE) down

.PHONY: restart
restart:
	$(DOCKER_COMPOSE) restart

.PHONY: ps
ps:
	$(DOCKER_COMPOSE) ps

.PHONY: logs
logs:
	$(DOCKER_COMPOSE) logs -f

.PHONY: logs-ai
logs-ai:
	$(DOCKER_COMPOSE) logs -f ai_worker

.PHONY: logs-bot
logs-bot:
	$(DOCKER_COMPOSE) logs -f tg_bot

.PHONY: logs-scraper
logs-scraper:
	$(DOCKER_COMPOSE) logs -f scraper

.PHONY: migrate-up
migrate-up:
	@make -C scraper migrate-up

.PHONY: migrate-down
migrate-down:
	@make -C scraper migrate-down

.PHONY: index
index:
	@make -C ai_worker index

.PHONY: test
test:
	cd scraper && go test -v -race ./...
	cd tg_bot && go test -v -race ./...
	cd ai_worker && $(if $(wildcard ai_worker/.venv/bin/python),.venv/bin/python,python3) -m unittest discover -s tests -v

.PHONY: clean
clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
