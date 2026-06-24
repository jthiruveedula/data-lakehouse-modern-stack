"""Gold layer aggregator — builds analytics-ready tables in BigQuery."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


@dataclass
class GoldTableSpec:
    table_name: str
    source_query: str
    partition_field: str
    cluster_fields: list[str]
    description: str = ""
    labels: dict[str, str] | None = None


@dataclass
class GoldBuildResult:
    table_name: str
    bq_table: str
    rows_written: int


class GoldAggregator:
    """Materialises Silver aggregations into BigQuery Gold tables."""

    def __init__(self, spark: SparkSession, gcp_project: str, bq_dataset: str) -> None:
        self.spark = spark
        self.project = gcp_project
        self.dataset = bq_dataset

    def build(self, spec: GoldTableSpec) -> GoldBuildResult:
        df = self.spark.sql(spec.source_query)
        bq_table = f"{self.project}.{self.dataset}.{spec.table_name}"

        df.write.format("bigquery") \
            .option("table", bq_table) \
            .option("partitionField", spec.partition_field) \
            .option("clusteredFields", ",".join(spec.cluster_fields)) \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_TRUNCATE") \
            .mode("overwrite") \
            .save()

        rows = df.count()
        logger.info("Gold table %s written: %d rows", bq_table, rows)
        return GoldBuildResult(table_name=spec.table_name, bq_table=bq_table, rows_written=rows)

    def build_incremental(self, spec: GoldTableSpec, watermark_col: str) -> GoldBuildResult:
        """Append only new partitions — cheaper than full overwrite."""
        bq_table = f"{self.project}.{self.dataset}.{spec.table_name}"
        last_ts = self._get_watermark(bq_table, watermark_col)

        query = f"""
            SELECT * FROM ({spec.source_query}) base
            WHERE {watermark_col} > '{last_ts}'
        """
        df = self.spark.sql(query)

        df.write.format("bigquery") \
            .option("table", bq_table) \
            .option("partitionField", spec.partition_field) \
            .option("clusteredFields", ",".join(spec.cluster_fields)) \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_APPEND") \
            .mode("append") \
            .save()

        rows = df.count()
        logger.info("Incremental Gold write %s: %d rows (watermark > %s)", bq_table, rows, last_ts)
        return GoldBuildResult(table_name=spec.table_name, bq_table=bq_table, rows_written=rows)

    def _get_watermark(self, bq_table: str, col: str) -> str:
        try:
            row = self.spark.read.format("bigquery") \
                .option("table", bq_table) \
                .load() \
                .selectExpr(f"MAX({col}) AS wm") \
                .first()
            return str(row["wm"]) if row and row["wm"] else "1970-01-01"
        except Exception:
            return "1970-01-01"

    def refresh_materialized_view(self, view_name: str) -> None:
        """Trigger a BigQuery scheduled query refresh by name."""
        from google.cloud import bigquery
        client = bigquery.Client(project=self.project)
        query = f"CALL BQ.REFRESH_MATERIALIZED_VIEW('{self.project}.{self.dataset}.{view_name}')"
        client.query(query).result()
        logger.info("Refreshed materialized view: %s", view_name)
