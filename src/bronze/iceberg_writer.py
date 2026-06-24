"""Apache Iceberg Bronze writer — hidden partitioning and snapshot-based ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class IcebergWriteResult:
    table_name: str
    catalog_table: str
    rows_written: int
    batch_id: str
    snapshot_id: int | None


class IcebergBronzeWriter:
    """Writes enriched records to Apache Iceberg Bronze tables using hidden partitioning."""

    def __init__(self, spark: SparkSession, catalog: str = "iceberg") -> None:
        self.spark = spark
        self.catalog = catalog

    def write(
        self,
        df: DataFrame,
        table_name: str,
        source_system: str,
        *,
        partition_spec: str = "days(_ingestion_ts)",
        sort_order: str | None = None,
    ) -> IcebergWriteResult:
        batch_id = str(uuid4())
        enriched = self._enrich(df, source_system, batch_id)
        catalog_table = f"{self.catalog}.bronze.{table_name}"

        self._ensure_table(enriched, catalog_table, partition_spec)

        writer = enriched.writeTo(catalog_table).using("iceberg")
        if sort_order:
            writer = writer.option("write.sort-order", sort_order)
        writer.append()

        rows = enriched.count()
        snapshot_id = self._latest_snapshot(catalog_table)
        logger.info(
            "Bronze Iceberg write: table=%s rows=%d snapshot=%s",
            catalog_table, rows, snapshot_id,
        )
        return IcebergWriteResult(
            table_name=table_name,
            catalog_table=catalog_table,
            rows_written=rows,
            batch_id=batch_id,
            snapshot_id=snapshot_id,
        )

    def _enrich(self, df: DataFrame, source: str, batch_id: str) -> DataFrame:
        return (
            df.withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source", F.lit(source))
            .withColumn("_batch_id", F.lit(batch_id))
        )

    def _ensure_table(self, df: DataFrame, catalog_table: str, partition_spec: str) -> None:
        try:
            df.writeTo(catalog_table) \
                .using("iceberg") \
                .partitionedBy(partition_spec) \
                .createOrReplace()
        except Exception:
            # Table already exists; schema evolution handled by Iceberg
            pass

    def _latest_snapshot(self, catalog_table: str) -> int | None:
        try:
            row = self.spark.sql(
                f"SELECT snapshot_id FROM {self.catalog}.bronze.snapshots "
                f"WHERE table_name='{catalog_table}' ORDER BY committed_at DESC LIMIT 1"
            ).first()
            return row["snapshot_id"] if row else None
        except Exception:
            return None

    def time_travel_snapshot(self, table_name: str, snapshot_id: int) -> DataFrame:
        return self.spark.read.format("iceberg") \
            .option("snapshot-id", snapshot_id) \
            .load(f"{self.catalog}.bronze.{table_name}")

    def expire_snapshots(self, table_name: str, retain_last: int = 10) -> None:
        catalog_table = f"{self.catalog}.bronze.{table_name}"
        self.spark.sql(
            f"CALL {self.catalog}.system.expire_snapshots("
            f"table => '{catalog_table}', retain_last => {retain_last})"
        )
        logger.info("Expired old snapshots for %s, retained last %d", catalog_table, retain_last)

    def rewrite_manifests(self, table_name: str) -> None:
        catalog_table = f"{self.catalog}.bronze.{table_name}"
        self.spark.sql(
            f"CALL {self.catalog}.system.rewrite_manifests(table => '{catalog_table}')"
        )
