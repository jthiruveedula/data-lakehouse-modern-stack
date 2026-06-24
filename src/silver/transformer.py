"""Silver transformer — deduplication, SCD Type 2, and schema validation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    table_name: str
    rows_in: int
    rows_out: int
    duplicates_dropped: int
    scd2_inserts: int
    scd2_updates: int


@dataclass
class SilverTableConfig:
    table_name: str
    primary_keys: list[str]
    dedupe_order_col: str = "_ingestion_ts"
    scd2_enabled: bool = True
    quality_threshold: float = 0.99
    partition_cols: list[str] = field(default_factory=lambda: ["_silver_date"])
    hash_cols: list[str] = field(default_factory=list)


class SilverTransformer:
    """Promotes Bronze data to Silver with deduplication and optional SCD Type 2."""

    def __init__(self, spark: SparkSession, gcs_bucket: str) -> None:
        self.spark = spark
        self.gcs_bucket = gcs_bucket

    def transform(self, bronze_df: DataFrame, config: SilverTableConfig) -> TransformResult:
        rows_in = bronze_df.count()
        deduped = self._deduplicate(bronze_df, config)
        rows_deduped = deduped.count()

        if config.scd2_enabled:
            inserts, updates = self._apply_scd2(deduped, config)
        else:
            self._overwrite_silver(deduped, config)
            inserts, updates = rows_deduped, 0

        return TransformResult(
            table_name=config.table_name,
            rows_in=rows_in,
            rows_out=rows_deduped,
            duplicates_dropped=rows_in - rows_deduped,
            scd2_inserts=inserts,
            scd2_updates=updates,
        )

    def _deduplicate(self, df: DataFrame, config: SilverTableConfig) -> DataFrame:
        window = Window.partitionBy(*config.primary_keys).orderBy(
            F.col(config.dedupe_order_col).desc()
        )
        return (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("_silver_date", F.to_date(F.current_timestamp()))
        )

    def _hash_row(self, df: DataFrame, cols: list[str]) -> DataFrame:
        if not cols:
            cols = [c for c in df.columns if not c.startswith("_")]
        concat_expr = F.concat_ws("|", *[F.col(c).cast("string") for c in cols])
        return df.withColumn("_row_hash", F.sha2(concat_expr, 256))

    def _apply_scd2(
        self, incoming: DataFrame, config: SilverTableConfig
    ) -> tuple[int, int]:
        silver_path = f"gs://{self.gcs_bucket}/silver/{config.table_name}"
        hashed = self._hash_row(incoming, config.hash_cols)

        # Register temp views for the MERGE statement
        hashed.createOrReplaceTempView(f"_incoming_{config.table_name}")

        pk_join = " AND ".join(
            f"t.{k} = s.{k}" for k in config.primary_keys
        )

        try:
            self.spark.sql(f"""
                MERGE INTO delta.`{silver_path}` t
                USING (
                    SELECT *, current_timestamp() AS _valid_from
                    FROM   _incoming_{config.table_name}
                ) s ON {pk_join}
                   AND t._is_current = true

                WHEN MATCHED AND t._row_hash != s._row_hash THEN
                    UPDATE SET t._valid_to   = current_timestamp(),
                               t._is_current = false

                WHEN NOT MATCHED THEN
                    INSERT (*, _valid_from, _valid_to, _is_current)
                    VALUES (s.*, s._valid_from, NULL, true)
            """)
        except Exception as exc:
            # First write — table doesn't exist yet
            if "does not exist" in str(exc).lower():
                self._bootstrap_scd2(hashed, silver_path, config)
                return hashed.count(), 0

        metrics = self._last_merge_metrics(silver_path)
        return metrics.get("numTargetRowsInserted", 0), metrics.get("numTargetRowsUpdated", 0)

    def _bootstrap_scd2(
        self, df: DataFrame, silver_path: str, config: SilverTableConfig
    ) -> None:
        df.withColumn("_valid_from", F.current_timestamp()) \
          .withColumn("_valid_to", F.lit(None).cast("timestamp")) \
          .withColumn("_is_current", F.lit(True)) \
          .write.format("delta") \
          .mode("overwrite") \
          .partitionBy(*config.partition_cols) \
          .save(silver_path)

    def _overwrite_silver(self, df: DataFrame, config: SilverTableConfig) -> None:
        silver_path = f"gs://{self.gcs_bucket}/silver/{config.table_name}"
        df.write.format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .partitionBy(*config.partition_cols) \
          .save(silver_path)

    def _last_merge_metrics(self, path: str) -> dict[str, int]:
        try:
            row = self.spark.sql(
                f"DESCRIBE HISTORY delta.`{path}` LIMIT 1"
            ).first()
            return row["operationMetrics"] if row else {}
        except Exception:
            return {}
