# Data Lakehouse Modern Stack

[![CI](https://github.com/jthiruveedula/data-lakehouse-modern-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/jthiruveedula/data-lakehouse-modern-stack/actions/workflows/ci.yml)
[![Pages](https://github.com/jthiruveedula/data-lakehouse-modern-stack/actions/workflows/pages.yml/badge.svg)](https://jthiruveedula.github.io/data-lakehouse-modern-stack)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Delta Lake 3.x](https://img.shields.io/badge/delta--lake-3.x-blue.svg)](https://delta.io)
[![Apache Iceberg 1.5](https://img.shields.io/badge/iceberg-1.5-blue.svg)](https://iceberg.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Production-grade open data lakehouse on GCP** — Delta Lake + Apache Iceberg, medallion architecture, dbt transformations, Trino query federation, and GenAI semantic search.

📖 **[Interactive Documentation Site →](https://jthiruveedula.github.io/data-lakehouse-modern-stack)**

---

## Architecture

```
Raw Sources ──► Bronze Layer ──► Silver Layer ──► Gold Layer ──► Query / AI
(Kafka, CDC,    (append-only,    (dbt + PySpark,  (star schema,  (Trino 430,
 REST APIs,      full fidelity,   SCD Type 2,      BigQuery,      LangChain RAG,
 Batch Files)    Delta/Iceberg)   quality gates)   materialized   Vertex AI)
                                                   views)
```

### Medallion Layer Details

| Layer  | Format          | Guarantee                    | Key Operations              |
|--------|----------------|------------------------------|-----------------------------|
| Bronze | Delta / Iceberg | Append-only, raw fidelity    | Partition by ingest date    |
| Silver | Delta           | Deduplicated, SCD Type 2     | MERGE, row hashing, tests   |
| Gold   | Delta / BigQuery | Analytics-ready, pre-aggregated | Star schema, KPIs, BQ    |

---

## Features

- **ACID Transactions** — Delta Lake optimistic concurrency + Iceberg snapshot isolation
- **Time Travel** — Query any past version or timestamp; zero additional storage
- **Schema Evolution** — Add/rename/reorder columns without rewriting history
- **SCD Type 2** — Full change history with `valid_from` / `valid_to` / `is_current`
- **dbt Transformations** — Incremental models, tests, docs, snapshots for all layers
- **Trino Federation** — Single SQL joins across GCS Delta, BigQuery, and PostgreSQL
- **GenAI Semantic Search** — Natural language → SQL via LangChain + Vertex AI
- **Data Catalog** — GCP DataPlex lineage, quality scores, column-level security
- **Infrastructure as Code** — Full Terraform for Dataproc, Composer, DataPlex, GKE
- **Cost Optimisation** — Z-order compaction, Iceberg rewrite, auto-scaling, spot workers

---

## Project Structure

```
data-lakehouse-modern-stack/
├── src/
│   ├── lakehouse_manager.py       # Core orchestrator (Bronze/Silver/Gold)
│   ├── ingestion/
│   │   ├── kafka_processor.py     # Confluent Kafka consumer + producer
│   │   └── api_ingester.py        # Paginated REST API ingester
│   ├── bronze/
│   │   ├── delta_writer.py        # Delta Lake Bronze write + OPTIMIZE
│   │   └── iceberg_writer.py      # Iceberg hidden-partition Bronze write
│   ├── silver/
│   │   └── transformer.py         # Deduplication + SCD Type 2 MERGE
│   ├── gold/
│   │   └── aggregator.py          # BigQuery Gold materialisation
│   ├── semantic_search/
│   │   ├── metadata_indexer.py    # Chroma vector store + catalog crawler
│   │   ├── query_engine.py        # NL → SQL via LangChain RAG
│   │   └── api.py                 # FastAPI /query endpoint
│   └── maintenance/
│       └── optimizer.py           # Delta OPTIMIZE/VACUUM + Iceberg compaction
├── dbt_project/
│   ├── models/bronze/             # Staging models
│   ├── models/silver/             # dim_customers, fct_orders (SCD2, incremental)
│   └── models/gold/               # agg_revenue_daily, agg_customer_ltv
├── infrastructure/
│   ├── main.tf                    # Dataproc, Composer, BigQuery, DataPlex
│   ├── variables.tf
│   └── outputs.tf
├── tests/
│   ├── conftest.py                # Shared fixtures (mock Spark, DataFrames)
│   ├── test_lakehouse_manager.py
│   ├── test_ingestion.py
│   └── test_silver_transformer.py
├── docker/Dockerfile              # Dev + production API images
├── docs/index.html                # GitHub Pages documentation site
├── requirements.txt
├── pyproject.toml
└── Makefile
```

---

## Quickstart

### Prerequisites

- Python 3.11+
- Java 17+ (for PySpark)
- GCP project with billing enabled
- `gcloud` CLI authenticated

### 1 — Install

```bash
git clone https://github.com/jthiruveedula/data-lakehouse-modern-stack
cd data-lakehouse-modern-stack
make install
```

### 2 — Configure

```bash
export GCP_PROJECT="your-project-id"
export GCS_BUCKET="your-lakehouse-bucket"
gcloud auth application-default login
```

### 3 — Provision Infrastructure

```bash
cd infrastructure/
terraform init
terraform apply -var="project_id=$GCP_PROJECT"
```

### 4 — Ingest to Bronze

```python
from src import MedallionPipelineOrchestrator, LakehouseConfig, LakehouseTable
from src import MedallionLayer, TableFormat

config = LakehouseConfig(
    gcp_project=os.environ["GCP_PROJECT"],
    gcs_bucket=os.environ["GCS_BUCKET"],
    bq_dataset="lakehouse_gold",
)
orch = MedallionPipelineOrchestrator(config)

table = LakehouseTable(
    name="orders",
    layer=MedallionLayer.BRONZE,
    table_format=TableFormat.DELTA,
    gcs_path=f"gs://{config.gcs_bucket}/bronze/orders",
    partition_cols=["_ingestion_date"],
    cluster_cols=["customer_id"],
    primary_keys=["order_id"],
    schema={"order_id": "string", "amount": "double"},
)

result = await orch.ingest_to_bronze(df, table, source_system="orders_api")
print(f"Wrote {result['rows_written']} rows to {result['gcs_path']}")
```

### 5 — Run dbt

```bash
cd dbt_project/
dbt deps
dbt run --select silver+    # Silver + Gold models
dbt test                     # Quality tests
dbt docs generate && dbt docs serve
```

### 6 — Start Semantic Search API

```bash
make start-api
# POST http://localhost:8080/query
# {"question": "What are the top 10 customers by lifetime revenue?"}
```

---

## Time Travel Examples

```sql
-- Delta Lake: query orders as of 7 days ago
SELECT * FROM delta.`gs://bucket/bronze/orders`
TIMESTAMP AS OF dateadd(day, -7, current_timestamp());

-- Iceberg: query using a specific snapshot ID
SELECT * FROM iceberg.bronze.orders
FOR VERSION AS OF 8475123456789;

-- Revert a bad write (Delta)
RESTORE TABLE delta.`gs://bucket/silver/customers`
TO VERSION AS OF 42;
```

---

## Delta Lake vs Apache Iceberg

| Feature               | Delta Lake 3.x          | Apache Iceberg 1.5       |
|-----------------------|-------------------------|--------------------------|
| ACID Transactions     | ✅ Optimistic CC         | ✅ Snapshot Isolation     |
| Time Travel           | ✅ VERSION / TIMESTAMP   | ✅ Snapshot-based         |
| Schema Evolution      | ✅ Full support          | ✅ + type widening        |
| Spark Integration     | ✅ Native (DataBricks)   | ✅ Full                   |
| Trino / Presto        | ✅ Good                  | ✅ Native first-class     |
| Partition Evolution   | ❌ Static only           | ✅ Hidden partitioning    |
| Branch / Tag API      | ❌                       | ✅                        |
| Flink Streaming       | ⚡ Limited               | ✅ Full                   |
| Ecosystem (2025)      | Databricks-driven        | Vendor-neutral            |

**Recommendation:** Delta Lake for Databricks/Spark-heavy workloads. Iceberg for Trino-first or multi-engine architectures.

---

## Tech Stack

| Component             | Technology                                   |
|-----------------------|----------------------------------------------|
| Table Formats         | Delta Lake 3.x + Apache Iceberg 1.5          |
| Compute               | GCP Dataproc (Spark 3.5)                     |
| Transformations       | dbt-spark + dbt-bigquery                     |
| Query Federation      | Trino 430 on GKE                             |
| CDC                   | Debezium + Apache Kafka                      |
| Data Catalog          | GCP DataPlex                                 |
| Semantic Search       | LangChain + Vertex AI + Chroma               |
| Orchestration         | Cloud Composer (Airflow 2.7)                 |
| Infrastructure        | Terraform 1.7+                               |
| Storage               | Google Cloud Storage                         |
| Analytics             | BigQuery                                     |
| API                   | FastAPI + uvicorn                            |

---

## Development

```bash
make lint          # Ruff linter
make format        # Auto-format
make type-check    # mypy
make test          # pytest
make test-cov      # pytest + coverage report
make docker-build  # Build Docker image
```

---

## Interview Talking Points

- **Delta vs Iceberg**: Delta Lake has better Spark integration; Iceberg has superior partition evolution and broader engine support. Iceberg's hidden partitioning prevents query mistakes from partition mismatches.
- **Medallion trade-offs**: Bronze is append-only for auditability; Silver handles deduplication + SCD; Gold optimizes for query performance. Cost is ~3× storage but analytical performance improves 10–100×.
- **Time travel use cases**: ML feature backfill, compliance audits, debugging incorrect transformations without restoring backups.
- **Trino federation**: Joins across BigQuery + GCS Iceberg + PostgreSQL in a single SQL query — enables data virtualization without ETL.
- **Small file problem**: Delta `OPTIMIZE` and Iceberg `rewrite_data_files` must be scheduled; target 128–512 MB file sizes for optimal Spark performance.

---

## License

[MIT](LICENSE)
