.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

BROWSER := uv run python -c "$$BROWSER_PYSCRIPT"

.PHONY: help
help: ## Shows this message.
	@echo "Pure-asyncio OpenTelemetry OTLP client."; \
	echo; \
	echo "Usage:"; \
	awk -F ':|##' '/^[^\t].+?:.*?##/ {\
		printf "\033[36m  make %-30s\033[0m %s\n", $$1, $$NF \
	}' $(MAKEFILE_LIST)

.PHONY: dev
dev: install ## Set up a development environment.
	uv run pre-commit install

.PHONY: install
install: ## Sync the virtualenv with every dependency and extra.
	uv sync --all-extras

.PHONY: lint
lint: lint-ruff lint-format lint-mypy ## Run all linters.

.PHONY: lint-ruff
lint-ruff: ## Run linting using Ruff.
	uv run ruff check

.PHONY: lint-format
lint-format: ## Check formatting using Ruff.
	uv run ruff format --check

.PHONY: lint-mypy
lint-mypy: ## Run type checking using MyPy (strict).
	uv run mypy

.PHONY: format
format: ## Reformat the code and apply safe Ruff fixes.
	uv run ruff check --fix; \
	uv run ruff format

.PHONY: pre-commit
pre-commit: ## Run every pre-commit hook against all files.
	uv run pre-commit run --all-files --show-diff-on-failure

.PHONY: test
test: ## Run the offline test suite with coverage.
	uv run pytest --cov=otlp_client --cov-report term \
		--cov-report html --cov-report xml:coverage.xml

.PHONY: test-integration
test-integration: collector-up ## Run the opt-in suite against a live collector.
	uv run pytest -m integration -v; \
	status=$$?; \
	$(MAKE) collector-down; \
	exit $$status

.PHONY: collector-up
collector-up: ## Start the otel-collector used by the integration suite.
	mkdir -p out; \
	docker compose -f docker-compose.test.yml up -d

.PHONY: collector-down
collector-down: ## Stop the otel-collector.
	docker compose -f docker-compose.test.yml down

.PHONY: coverage
coverage: test ## Check code coverage quickly with the default Python.
	$(BROWSER) htmlcov/index.html

.PHONY: clean clean-all
clean: clean-build clean-pyc clean-test ## Removes build, test, coverage and Python artifacts.
clean-all: clean clean-venv ## Removes all venv, build, test, coverage and Python artifacts.

.PHONY: clean-build
clean-build: ## Removes build artifacts.
	rm -fr build/; \
	rm -fr dist/; \
	rm -fr .eggs/; \
	find . -name '*.egg-info' -exec rm -fr {} +; \
	find . -name '*.egg' -exec rm -fr {} +;

.PHONY: clean-pyc
clean-pyc: ## Removes Python file artifacts.
	find . -name '*.pyc' -delete; \
	find . -name '*.pyo' -delete; \
	find . -name '*~' -delete; \
	find . -name '__pycache__' -exec rm -fr {} +;

.PHONY: clean-test
clean-test: ## Removes test, cache and coverage artifacts.
	rm -f .coverage coverage.xml; \
	rm -fr htmlcov/; \
	rm -fr .pytest_cache .mypy_cache .ruff_cache .hypothesis; \
	rm -fr out/;

.PHONY: clean-venv
clean-venv: ## Removes the Python virtual environment.
	rm -fr .venv/;

.PHONY: dist
dist: clean ## Builds source and wheel package.
	uv build; \
	ls -l dist;

.PHONY: release
release: dist ## Publishes to PyPI. CI does this on release; use for a manual push.
	uv publish
