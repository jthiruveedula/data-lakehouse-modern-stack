.PHONY: install lint format test test-cov dbt-run dbt-test tf-init tf-plan tf-apply docker-build clean help

PYTHON   := python3
PIP      := pip
DBT_DIR  := dbt_project
TF_DIR   := infrastructure
DOCKER_TAG := data-lakehouse:latest

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install all Python dependencies
	$(PIP) install -r requirements.txt

install-dev: ## Install dev dependencies
	$(PIP) install -r requirements.txt ruff mypy pytest pytest-cov pytest-asyncio pytest-mock

lint: ## Run ruff linter
	ruff check src/ tests/

format: ## Auto-format code with ruff
	ruff format src/ tests/
	ruff check --fix src/ tests/

type-check: ## Run mypy type checking
	mypy src/ --ignore-missing-imports

test: ## Run unit tests
	pytest tests/ -v

test-cov: ## Run tests with coverage report
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html:htmlcov
	@echo "Coverage report: htmlcov/index.html"

dbt-deps: ## Install dbt packages
	cd $(DBT_DIR) && dbt deps

dbt-run: ## Run all dbt models
	cd $(DBT_DIR) && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd $(DBT_DIR) && dbt test --profiles-dir .

dbt-docs: ## Generate and serve dbt docs
	cd $(DBT_DIR) && dbt docs generate --profiles-dir . && dbt docs serve

tf-init: ## Initialize Terraform
	cd $(TF_DIR) && terraform init

tf-plan: ## Terraform plan (set TF_VAR_project)
	cd $(TF_DIR) && terraform plan

tf-apply: ## Terraform apply
	cd $(TF_DIR) && terraform apply

docker-build: ## Build Docker image
	docker build -f docker/Dockerfile -t $(DOCKER_TAG) .

docker-run: ## Run Docker container
	docker run --rm -it $(DOCKER_TAG)

ingest-bronze: ## Run a test Bronze ingestion (requires GCP credentials)
	$(PYTHON) -m src.ingestion.api_ingester

start-api: ## Start the semantic search FastAPI server
	uvicorn src.semantic_search.api:app --reload --host 0.0.0.0 --port 8080

clean: ## Clean generated files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs $(DBT_DIR)/.user.yml
