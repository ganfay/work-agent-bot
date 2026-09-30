VENV := .venv
PYTHON := $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)
PIP := $(if $(wildcard $(VENV)/bin/pip),$(VENV)/bin/pip,pip)

.PHONY: help
help:
	@echo "Available commands in AI Worker:"
	@echo "  make run        - Start the AI Worker service (auto-indexes vectors if empty)"
	@echo "  make index      - Manually re-generate and index code embeddings into pgvector"
	@echo "  make install    - Install python dependencies from requirements.txt"
	@echo "  make clean      - Remove __pycache__ and temporary Python files"

.PHONY: run
run:
	$(PYTHON) main.py

.PHONY: index
index:
	$(PYTHON) rag/indexer.py

.PHONY: install
install:
	$(PIP) install -r requirements.txt

.PHONY: clean
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
