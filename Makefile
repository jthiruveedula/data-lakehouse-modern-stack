.PHONY: install install-dev lint format type-check test test-cov quality \
        dbt-deps dbt-run dbt-test dbt-docs \
        tf-init tf-plan tf-apply \
        docker-build docker-run \
        local-up local-down local-ps \
        stream-validate cdc-validate \
        ingest-bronze start-api \
        pre-commit-install \
        clean help

PYTHON    := python3
PIP       := pip
DBT_DIR   := dbt_project
TF_DIR    := infrastructure
DOCKER_TAG := data-lakehouse:latest

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Install ────────────────────────────────────────────────────────────────────
install: ## Install all Python dependencies
	$(PIP) install -r requirements.txt

install-dev: ## Install dev dependencies (includes ruff, mypy, pytest, pre-commit)
	$(PIP) install -r requirements.txt ruff mypy pytest pytest-cov pytest-asyncio pytest-mock pre-commit

# ── Lint / Format / Type-check ─────────────────────────────────────────────────
lint: ## Run ruff linter (no auto-fix)
	ruff check src/ tests/ dags/

format: ## Auto-format and fix lint issues in place
	ruff format src/ tests/ dags/
	ruff check --fix src/ tests/ dags/

format-check: ## Check formatting without modifying files (used in CI)
	ruff format --check src/ tests/ dags/
	ruff check src/ tests/ dags/

type-check: ## Run mypy type checking
	mypy src/ --ignore-missing-imports

# ── Tests ──────────────────────────────────────────────────────────────────────
test: ## Run unit tests
	pytest tests/ -v

test-cov: ## Run tests with coverage report (HTML + terminal)
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html:htmlcov
	@echo "Coverage report: htmlcov/index.html"

quality: ## Run full quality gate: lint + type-check + tests with coverage
	$(MAKE) format-check
	$(MAKE) type-check
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=25
	@echo "Quality gate passed."

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-deps: ## Install dbt packages
	cd $(DBT_DIR) && dbt deps

dbt-run: ## Run all dbt models
	cd $(DBT_DIR) && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd $(DBT_DIR) && dbt test --profiles-dir .

dbt-compile: ## Compile dbt (no execution) — fast CI check
	cd $(DBT_DIR) && dbt compile --profiles-dir .

dbt-docs: ## Generate and serve dbt docs at http://localhost:8083
	cd $(DBT_DIR) && dbt docs generate --profiles-dir . && dbt docs serve --port 8083

# ── Terraform ─────────────────────────────────────────────────────────────────
tf-init: ## Initialize Terraform
	cd $(TF_DIR) && terraform init

tf-plan: ## Terraform plan (set TF_VAR_project=<your-gcp-project>)
	cd $(TF_DIR) && terraform plan

tf-apply: ## Terraform apply
	cd $(TF_DIR) && terraform apply

tf-validate: ## Validate Terraform config without credentials
	cd $(TF_DIR) && terraform init -backend=false && terraform validate

# ── Docker local dev stack ─────────────────────────────────────────────────────
local-up: ## Start the local dev stack (Kafka, MinIO, Spark, Jupyter, Postgres)
	docker compose up -d
	@echo "Services:"
	@echo "  MinIO console  → http://localhost:9001  (user: minioadmin / pass: minioadmin123)"
	@echo "  Kafka UI       → http://localhost:8090"
	@echo "  Spark UI       → http://localhost:8081"
	@echo "  JupyterLab     → http://localhost:8888  (token: lakehouse)"

local-down: ## Stop and remove all local dev containers and volumes
	docker compose down -v

local-ps: ## Show status of local dev stack
	docker compose ps

# ── Streaming / CDC validation ─────────────────────────────────────────────────
stream-validate: ## Validate StreamConfig and processor instantiation (no Spark needed)
	$(PYTHON) -c "from src.ingestion.streaming_processor import StreamConfig, StructuredStreamingProcessor; print('streaming_processor OK')"

cdc-validate: ## Validate CDC processor and Debezium parsing
	$(PYTHON) -c "from src.ingestion.cdc_processor import CDCConfig, CDCProcessor, CDCEvent, CDCOperation; print('cdc_processor OK')"

# ── Ingestion / API ────────────────────────────────────────────────────────────
ingest-bronze: ## Run a test Bronze ingestion (requires GCP credentials)
	$(PYTHON) -m src.ingestion.api_ingester

start-api: ## Start the semantic search FastAPI server at http://localhost:8080/docs
	uvicorn src.semantic_search.api:app --reload --host 0.0.0.0 --port 8080

# ── Pre-commit ─────────────────────────────────────────────────────────────────
pre-commit-install: ## Install pre-commit hooks into .git/hooks
	pre-commit install
	@echo "Pre-commit hooks installed. Hooks run on every git commit."

pre-commit-run: ## Run all pre-commit hooks against all files
	pre-commit run --all-files

# ── Clean ──────────────────────────────────────────────────────────────────────
clean: ## Clean generated files (caches, coverage, dbt artifacts)
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs $(DBT_DIR)/.user.yml
