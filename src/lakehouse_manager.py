"""Data Lakehouse Modern Stack - Core Manager
Orchestrates Bronze/Silver/Gold medallion architecture using
Delta Lake + Apache Iceberg on GCS with dbt transformations,
Trino federation, and CDC via Debezium + Kafka.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


class MedallionLayer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class TableFormat(StrEnum):
    DELTA = "delta"
    ICEBERG = "iceberg"
    PARQUET = "parquet"


@dataclass
class LakehouseTable:
    name: str
    layer: MedallionLayer
    format: TableFormat
    gcs_path: str
    bq_external_table: str = ""
    partition_columns: list[str] = field(default_factory=list)
    cluster_columns: list[str] = field(default_factory=list)
    scd_type: int = 1  # 1 or 2
    primary_keys: list[str] = field(default_factory=list)
    schema: list[dict] = field(default_factory=list)


@dataclass
class LakehouseConfig:
    project_id: str
    gcs_bucket: str
    bq_dataset: str = "lakehouse"
    location: str = "us-central1"
    default_format: TableFormat = TableFormat.ICEBERG


class MedallionPipelineOrchestrator:
    """Orchestrates data through Bronze -> Silver -> Gold layers."""

    def __init__(self, config: LakehouseConfig) -> None:
        self.config = config
        self.bq = bigquery.Client(project=config.project_id)
        self.gcs = storage.Client(project=config.project_id)
        self._tables: dict[str, LakehouseTable] = {}

    def register_table(self, table: LakehouseTable) -> None:
        self._tables[table.name] = table
        logger.info("Registered table %s in %s layer.", table.name, table.layer.value)

    # ------------------------------------------------------------------
    # Bronze Layer - Raw ingestion
    # ------------------------------------------------------------------
    def ingest_to_bronze(
        self,
        table_name: str,
        data: list[dict],
        source: str = "api",
    ) -> dict:
        """Append-only raw ingestion to Bronze layer."""
        table = self._get_table(table_name, MedallionLayer.BRONZE)
        timestamp = time.strftime("%Y/%m/%d/%H", time.gmtime())
        gcs_path = f"{table.gcs_path}/{timestamp}/batch_{int(time.time())}.json"

        # Add metadata
        enriched_rows = [
            {
                **row,
                "_ingestion_ts": time.time(),
                "_source": source,
                "_batch_id": f"{source}_{int(time.time())}",
            }
            for row in data
        ]

        bucket = self.gcs.bucket(self.config.gcs_bucket)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(
            "\n".join(json.dumps(r) for r in enriched_rows),
            content_type="application/json",
        )
        logger.info("Ingested %d rows to bronze: gs://%s/%s", len(data), self.config.gcs_bucket, gcs_path)
        return {"rows": len(data), "gcs_path": gcs_path, "layer": "bronze"}

    # ------------------------------------------------------------------
    # Silver Layer - Deduplication + SCD Type 2
    # ------------------------------------------------------------------
    def transform_to_silver(
        self,
        table_name: str,
        bronze_table_ref: str,
    ) -> dict:
        """Transform Bronze -> Silver with dedup, type casting, and SCD2."""
        table = self._get_table(table_name, MedallionLayer.SILVER)
        pks = ", ".join(table.primary_keys) or "id"

        if table.scd_type == 2:
            sql = self._build_scd2_merge(table, bronze_table_ref)
        else:
            sql = f"""
                CREATE OR REPLACE TABLE `{self.config.project_id}.{self.config.bq_dataset}.{table_name}_silver`
                AS
                SELECT * EXCEPT(row_num)
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {pks}
                               ORDER BY _ingestion_ts DESC
                           ) AS row_num
                    FROM `{bronze_table_ref}`
                    WHERE _ingestion_ts IS NOT NULL
                )
                WHERE row_num = 1
            """

        job = self.bq.query(sql)
        job.result()
        logger.info("Silver transformation complete for %s.", table_name)
        return {"table": table_name, "layer": "silver", "job_id": job.job_id}

    def _build_scd2_merge(self, table: LakehouseTable, source_ref: str) -> str:
        pks = " AND ".join(f"T.{pk} = S.{pk}" for pk in table.primary_keys)
        target = f"`{self.config.project_id}.{self.config.bq_dataset}.{table.name}_silver`"
        return f"""
            MERGE {target} T
            USING `{source_ref}` S
            ON {pks} AND T.is_current = TRUE
            WHEN MATCHED AND T._row_hash != TO_HEX(MD5(TO_JSON_STRING(S))) THEN
                UPDATE SET T.valid_to = CURRENT_TIMESTAMP(), T.is_current = FALSE
            WHEN NOT MATCHED THEN
                INSERT (
                    {', '.join(f.get('name', '') for f in table.schema)},
                    valid_from, valid_to, is_current, _row_hash
                )
                VALUES (
                    {', '.join(f'S.{f.get("name", "")}' for f in table.schema)},
                    CURRENT_TIMESTAMP(), NULL, TRUE,
                    TO_HEX(MD5(TO_JSON_STRING(S)))
                )
        """

    # ------------------------------------------------------------------
    # Gold Layer - Aggregated analytics tables
    # ------------------------------------------------------------------
    def build_gold_table(
        self,
        table_name: str,
        sql: str,
        partition_by: str = "DATE(_PARTITIONTIME)",
    ) -> dict:
        """Build Gold layer aggregated table from Silver sources."""
        target = f"`{self.config.project_id}.{self.config.bq_dataset}.{table_name}_gold`"
        create_sql = f"""
            CREATE OR REPLACE TABLE {target}
            PARTITION BY {partition_by}
            AS ({sql})
        """
        job = self.bq.query(create_sql)
        job.result()
        logger.info("Gold table %s built. Job: %s", table_name, job.job_id)
        return {"table": f"{table_name}_gold", "layer": "gold", "job_id": job.job_id}

    # ------------------------------------------------------------------
    # BQ External Table Registration for Iceberg/Delta
    # ------------------------------------------------------------------
    def register_iceberg_table(
        self,
        table_name: str,
        gcs_uri: str,
        schema: list[dict],
    ) -> None:
        """Register an Iceberg table in BigQuery as external table."""
        sql = f"""
            CREATE OR REPLACE EXTERNAL TABLE
              `{self.config.project_id}.{self.config.bq_dataset}.{table_name}`
            WITH CONNECTION `{self.config.project_id}.{self.config.location}.bq-connection`
            OPTIONS (
              format = 'ICEBERG',
              uris = ['{gcs_uri}']
            )
        """
        self.bq.query(sql).result()
        logger.info("Registered Iceberg external table %s at %s.", table_name, gcs_uri)

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------
    def get_lineage(self, table_name: str) -> dict:
        """Return simple data lineage map for a table."""
        return {
            "table": table_name,
            "lineage": [
                {"layer": "source", "type": "API/Kafka/CDC"},
                {"layer": "bronze", "type": "GCS raw JSON", "path": f"gs://{self.config.gcs_bucket}/bronze/{table_name}/"},
                {"layer": "silver", "type": "BQ table", "ref": f"{self.config.bq_dataset}.{table_name}_silver"},
                {"layer": "gold", "type": "BQ table", "ref": f"{self.config.bq_dataset}.{table_name}_gold"},
            ],
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_table(self, name: str, layer: MedallionLayer) -> LakehouseTable:
        if name not in self._tables:
            self._tables[name] = LakehouseTable(
                name=name, layer=layer,
                format=self.config.default_format,
                gcs_path=f"{layer.value}/{name}",
            )
        return self._tables[name]

