# Data Lakehouse Modern Stack

> **Production-grade open data lakehouse** built on Delta Lake + Apache Iceberg with medallion architecture, dbt transformations, Trino query federation, and a GenAI semantic search layer — all on GCP.

![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue) ![Iceberg](https://img.shields.io/badge/Apache-Iceberg-orange) ![dbt](https://img.shields.io/badge/dbt-core-red) ![Trino](https://img.shields.io/badge/Trino-Query-purple) ![GCP](https://img.shields.io/badge/GCP-DataProc-blue)

---

## Architecture: Medallion + Open Table Format

```
RAW SOURCES
  APIs / Kafka / Files / CDC (Debezium)
         |
         v
+--------+------------------------------------------+
|                BRONZE LAYER (Raw)                 |
|  GCS Parquet / Delta Lake / Iceberg tables         |
|  Full fidelity, append-only, partitioned by date   |
+--------+------------------------------------------+
         |
         v (dbt + PySpark)
+--------+------------------------------------------+
|                SILVER LAYER (Cleaned)             |
|  Deduplicated, type-cast, SCD Type 2 dimensions    |
|  Great Expectations quality gates                  |
+--------+------------------------------------------+
         |
         v (dbt)
+--------+------------------------------------------+
|                GOLD LAYER (Business)              |
|  Fact/Dimension star schema                        |
|  Pre-aggregated metrics, materialized views        |
+--------+------------------------------------------+
         |
    +----+------+
    |           |
    v           v
 Trino      Semantic
 (SQL)      Search
            (RAG)
```

---

## Key Features

- **Delta Lake + Iceberg**: Comparison of both open table formats side-by-side; ACID transactions, schema evolution, time travel
- **Medallion Architecture**: Bronze/Silver/Gold with automated promotion triggers and quality gates
- **dbt Transformations**: Full dbt project with sources, models, tests, docs, and snapshots (SCD Type 2)
- **Trino Query Federation**: Single SQL interface across GCS, BigQuery, and PostgreSQL simultaneously
- **Time Travel Queries**: Query any table as-of any timestamp (audit, debugging, ML feature backfill)
- **GenAI Semantic Search**: LLM-powered natural language search across all Gold layer datasets
- **Data Catalog**: Automated metadata registration in DataPlex + custom dbt docs site
- **Cost Optimization**: Iceberg compaction jobs + Delta OPTIMIZE to manage small file problem

---

## Open Table Format Comparison (Built-in)

| Feature | Delta Lake | Apache Iceberg |
|---------|-----------|----------------|
| Time Travel | Yes (version + timestamp) | Yes (snapshot) |
| Schema Evolution | Yes (MERGE) | Yes (full) |
| Partition Evolution | Limited | Yes (transparent) |
| ACID Transactions | Yes | Yes |
| Engine Support | Spark, Presto, Athena | Spark, Trino, Flink |
| Streaming | Delta Streaming | Iceberg Streaming |
| Branching/Tagging | Yes (v3) | Yes (branches/tags) |
| GCP Native | Dataproc | Dataproc + BigLake |

---

## Project Structure

```
data-lakehouse-modern-stack/
|-- ingestion/
|   |-- kafka_consumer.py         # Kafka -> Bronze layer
|   |-- api_ingestion.py          # REST API -> Bronze layer
|   `-- cdc_processor.py          # Debezium CDC -> Bronze
|-- bronze/
|   |-- delta_writer.py           # Delta Lake table writes
|   `-- iceberg_writer.py         # Iceberg table writes
|-- dbt_project/                  # Full dbt project
|   |-- models/
|   |   |-- staging/              # Silver layer transforms
|   |   |-- marts/                # Gold layer aggregations
|   |   `-- snapshots/            # SCD Type 2 snapshots
|   |-- tests/
|   |-- macros/
|   `-- dbt_project.yml
|-- trino/
|   |-- catalog_configs/          # Trino catalog YAML files
|   `-- federation_queries/       # Example cross-source SQL
|-- semantic_search/
|   |-- metadata_indexer.py       # Index table/column metadata
|   |-- nl_query_engine.py        # Natural language -> SQL
|   `-- search_api.py             # FastAPI search endpoint
|-- maintenance/
|   |-- delta_optimize.py         # OPTIMIZE + VACUUM jobs
|   |-- iceberg_compact.py        # Iceberg compaction + expiry
|   `-- partition_manager.py      # Dynamic partition management
|-- infrastructure/
|   |-- dataproc_cluster.tf       # Terraform for Dataproc
|   |-- gcs_buckets.tf            # GCS bucket setup
|   `-- trino_helm.yaml           # Trino on GKE
|-- notebooks/
|   |-- 01_delta_time_travel.ipynb
|   |-- 02_iceberg_schema_evolution.ipynb
|   `-- 03_trino_federation.ipynb
`-- README.md
```

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Open Table Format | Delta Lake 3.x + Apache Iceberg 1.5 |
| Compute | GCP Dataproc (Spark 3.5) |
| Transformations | dbt-spark + dbt-bigquery |
| Query Engine | Trino 430 (GKE) |
| CDC | Debezium + Kafka |
| Data Catalog | GCP Dataplex |
| Semantic Search | LangChain + Vertex AI Embeddings |
| IaC | Terraform |
| Orchestration | Cloud Composer (Airflow) |
| Storage | Google Cloud Storage (GCS) |

---

## GenAI Semantic Search Layer

Users can search the lakehouse using natural language:

```
Query: "Show me revenue by region for Q4 last year"

Semantic Search Result:
  Table: gold.fact_sales_summary
  Suggested SQL:
    SELECT region, SUM(revenue) as total_revenue
    FROM gold.fact_sales_summary
    WHERE year = 2025 AND quarter = 4
    GROUP BY region
    ORDER BY total_revenue DESC
  
  Related tables: gold.dim_region, gold.dim_product
```

---

## Interview Talking Points

- **Delta vs Iceberg**: Delta Lake has better Spark integration and DML performance; Iceberg has superior partition evolution and broader engine support (Trino, Flink). Iceberg's hidden partitioning prevents query mistakes from partition mismatches
- **Medallion architecture trade-offs**: Bronze is append-only for auditability; Silver handles deduplication + SCD; Gold optimizes for query performance. The cost is 3x storage but analytical performance improves 10-100x
- **Time travel use cases**: ML feature backfill, compliance audits, debugging incorrect transformations without restoring backups
- **Trino federation**: Joins across BigQuery + GCS Iceberg + PostgreSQL in a single SQL query — enables data virtualization without ETL for exploratory analysis
- **Small file problem**: Critical lakehouse operational challenge; Delta OPTIMIZE and Iceberg compaction must be scheduled; target 128MB-512MB file sizes for optimal Spark performance

