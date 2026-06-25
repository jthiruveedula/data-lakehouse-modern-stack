"""Delta Lake Bronze writer — append-only, metadata-enriched ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class DeltaWriteResult:
    table_name: str
    gcs_path: str
    rows_written: int
    batch_id: str
    version: int


class DeltaBronzeWriter:
    """Writes enriched records to a Delta Lake Bronze table on GCS."""

    METADATA_COLS = ("_ingestion_ts", "_source", "_batch_id", "_ingestion_date")

    def __init__(self, spark: SparkSession, gcs_bucket: str) -> None:
        self.spark = spark
        self.gcs_bucket = gcs_bucket

    def write(
        self,
        df: DataFrame,
        table_name: str,
        source_system: str,
        *,
        partition_cols: list[str] | None = None,
        z_order_cols: list[str] | None = None,
    ) -> DeltaWriteResult:
        batch_id = str(uuid4())
        enriched = self._enrich(df, source_system, batch_id)
        gcs_path = f"gs://{self.gcs_bucket}/bronze/{table_name}"
        parts = partition_cols or ["_ingestion_date"]

        enriched.write.format("delta").mode("append").partitionBy(*parts).option(
            "delta.autoOptimize.optimizeWrite", "true"
        ).option("delta.autoOptimize.autoCompact", "true").save(gcs_path)

        version = self._current_version(gcs_path)
        rows = enriched.count()
        logger.info(
            "Bronze Delta write: table=%s rows=%d version=%d path=%s",
            table_name,
            rows,
            version,
            gcs_path,
        )

        if z_order_cols:
            self.spark.sql(f"OPTIMIZE delta.`{gcs_path}` ZORDER BY ({', '.join(z_order_cols)})")

        return DeltaWriteResult(
            table_name=table_name,
            gcs_path=gcs_path,
            rows_written=rows,
            batch_id=batch_id,
            version=version,
        )

    def _enrich(self, df: DataFrame, source: str, batch_id: str) -> DataFrame:
        return (
            df.withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source", F.lit(source))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingestion_date", F.to_date(F.current_timestamp()))
        )

    def _current_version(self, path: str) -> int:
        try:
            history = self.spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1")
            return history.first()["version"]  # type: ignore[index]
        except Exception:
            return -1

    def time_travel(self, table_name: str, version: int) -> DataFrame:
        path = f"gs://{self.gcs_bucket}/bronze/{table_name}"
        return self.spark.read.format("delta").option("versionAsOf", version).load(path)

    def optimize(self, table_name: str) -> None:
        path = f"gs://{self.gcs_bucket}/bronze/{table_name}"
        self.spark.sql(f"OPTIMIZE delta.`{path}`")
        self.spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
        logger.info("Optimized Bronze Delta table: %s", table_name)
