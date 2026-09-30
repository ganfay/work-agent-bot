# Detect virtual environment if it exists, otherwise fall back to system python
VENV := .venv
PYTHON := $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)
PIP := $(if $(wildcard $(VENV)/bin/pip),$(VENV)/bin/pip,pip)

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make run        - Start the AI Worker service"
	@echo "  make install    - Install python dependencies from requirements.txt"
	@echo "  make clean      - Remove __pycache__ and temporary Python files"

.PHONY: run
run:
	$(PYTHON) main.py

.PHONY: install
install:
	$(PIP) install -r requirements.txt

.PHONY: clean
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
