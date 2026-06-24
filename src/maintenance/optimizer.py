"""LakehouseOptimizer — small file compaction, VACUUM, and statistics collection."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

TableFormat = Literal["delta", "iceberg"]


@dataclass
class OptimizeResult:
    table_name: str
    format: TableFormat
    files_before: int
    files_after: int
    bytes_removed: int
    duration_s: float


class LakehouseOptimizer:
    """Runs compaction, Z-ordering, and VACUUM on Delta / Iceberg tables."""

    DEFAULT_RETAIN_HOURS = 168  # 7 days
    DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024  # 128 MB

    def __init__(self, spark: SparkSession, gcs_bucket: str, catalog: str = "iceberg") -> None:
        self.spark = spark
        self.gcs_bucket = gcs_bucket
        self.catalog = catalog

    # ── Delta operations ────────────────────────────────────────────────────

    def optimize_delta(
        self,
        table_name: str,
        layer: str = "bronze",
        *,
        z_order_cols: list[str] | None = None,
    ) -> None:
        path = f"gs://{self.gcs_bucket}/{layer}/{table_name}"
        if z_order_cols:
            cols = ", ".join(z_order_cols)
            self.spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
            logger.info("OPTIMIZE + ZORDER(%s) on %s/%s", cols, layer, table_name)
        else:
            self.spark.sql(f"OPTIMIZE delta.`{path}`")
            logger.info("OPTIMIZE on delta %s/%s", layer, table_name)

    def vacuum_delta(
        self,
        table_name: str,
        layer: str = "bronze",
        retain_hours: int = DEFAULT_RETAIN_HOURS,
    ) -> None:
        path = f"gs://{self.gcs_bucket}/{layer}/{table_name}"
        self.spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
        logger.info("VACUUM delta %s/%s retain=%dh", layer, table_name, retain_hours)

    def delta_table_stats(self, table_name: str, layer: str = "bronze") -> dict:
        path = f"gs://{self.gcs_bucket}/{layer}/{table_name}"
        detail = self.spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
        return {
            "num_files": detail["numFiles"] if detail else None,
            "size_bytes": detail["sizeInBytes"] if detail else None,
            "partitioning": detail["partitionColumns"] if detail else None,
        }

    # ── Iceberg operations ───────────────────────────────────────────────────

    def rewrite_iceberg(
        self,
        table_name: str,
        schema: str = "bronze",
        *,
        target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE,
        strategy: str = "sort",
        sort_order: str = "zorder(id)",
    ) -> None:
        full_table = f"{self.catalog}.{schema}.{table_name}"
        self.spark.sql(f"""
            CALL {self.catalog}.system.rewrite_data_files(
                table           => '{full_table}',
                strategy        => '{strategy}',
                sort_order      => '{sort_order}',
                options         => map(
                    'target-file-size-bytes', '{target_file_size_bytes}',
                    'min-input-files', '5'
                )
            )
        """)
        logger.info("Rewrite data files on Iceberg %s", full_table)

    def expire_iceberg_snapshots(
        self, table_name: str, schema: str = "bronze", retain_last: int = 10
    ) -> None:
        full_table = f"{self.catalog}.{schema}.{table_name}"
        self.spark.sql(
            f"CALL {self.catalog}.system.expire_snapshots("
            f"table => '{full_table}', retain_last => {retain_last})"
        )
        logger.info("Expired snapshots for %s, kept last %d", full_table, retain_last)

    def rewrite_iceberg_manifests(self, table_name: str, schema: str = "bronze") -> None:
        full_table = f"{self.catalog}.{schema}.{table_name}"
        self.spark.sql(
            f"CALL {self.catalog}.system.rewrite_manifests(table => '{full_table}')"
        )

    # ── Full maintenance sweep ───────────────────────────────────────────────

    def run_full_maintenance(self, tables: list[dict]) -> list[str]:
        """Run all maintenance ops for a table manifest.

        Each entry: {"name": str, "layer": str, "format": "delta"|"iceberg"}
        """
        report = []
        for t in tables:
            name, layer, fmt = t["name"], t["layer"], t["format"]
            try:
                if fmt == "delta":
                    self.optimize_delta(name, layer)
                    self.vacuum_delta(name, layer)
                elif fmt == "iceberg":
                    self.rewrite_iceberg(name, layer)
                    self.expire_iceberg_snapshots(name, layer)
                report.append(f"OK  {fmt:8s} {layer}/{name}")
            except Exception as exc:
                report.append(f"ERR {fmt:8s} {layer}/{name}: {exc}")
                logger.exception("Maintenance failed for %s/%s", layer, name)
        return report
