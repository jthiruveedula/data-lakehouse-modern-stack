"""Spark Structured Streaming ingestion — Kafka → Bronze Delta Lake."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """Configuration for a single Kafka → Bronze streaming pipeline."""

    kafka_bootstrap_servers: str
    topics: list[str]
    checkpoint_location: str
    output_path: str
    trigger_interval: str = "60 seconds"
    max_offsets_per_trigger: int = 10_000
    starting_offsets: str = "latest"
    output_format: str = "delta"
    partition_columns: list[str] = field(default_factory=lambda: ["_ingestion_date"])
    watermark_delay: str = "10 minutes"
    schema_registry_url: str | None = None


class StructuredStreamingProcessor:
    """Low-latency Kafka-to-Bronze pipeline using Spark Structured Streaming.

    Each micro-batch is enriched with Kafka metadata and Lakehouse standard
    columns (_ingestion_ts, _ingestion_date, _kafka_offset, etc.) before being
    appended to the Bronze Delta table.
    """

    def __init__(self, spark: Any, config: StreamConfig) -> None:
        self.spark = spark
        self.config = config
        self._query: Any = None

    def start(self) -> None:
        """Build and start the streaming query."""
        raw_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("subscribe", ",".join(self.config.topics))
            .option("startingOffsets", self.config.starting_offsets)
            .option("maxOffsetsPerTrigger", self.config.max_offsets_per_trigger)
            .option("failOnDataLoss", "false")
            .load()
        )

        enriched = self._enrich(raw_df)

        self._query = (
            enriched.writeStream.format(self.config.output_format)
            .option("checkpointLocation", self.config.checkpoint_location)
            .option("path", self.config.output_path)
            .option("mergeSchema", "true")
            .partitionBy(*self.config.partition_columns)
            .trigger(processingTime=self.config.trigger_interval)
            .outputMode("append")
            .start()
        )
        logger.info("Streaming query started: id=%s", self._query.id)

    def _enrich(self, df: Any) -> Any:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        return (
            df.select(
                F.col("key").cast(StringType()).alias("_message_key"),
                F.col("value").cast(StringType()).alias("_raw_payload"),
                F.col("topic").alias("_kafka_topic"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
                F.col("timestamp").alias("_event_ts"),
            )
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_ts")))
            .withColumn("_batch_id", F.expr("uuid()"))
            .withWatermark("_ingestion_ts", self.config.watermark_delay)
        )

    def stop(self) -> None:
        """Gracefully stop the streaming query."""
        if self._query and self._query.isActive:
            self._query.stop()
            logger.info("Streaming query stopped")

    def await_termination(self, timeout_ms: int | None = None) -> bool:
        """Block until the streaming query terminates or timeout elapses."""
        if self._query is None:
            return False
        if timeout_ms is not None:
            return self._query.awaitTermination(timeout_ms)
        self._query.awaitTermination()
        return True

    @property
    def status(self) -> dict[str, Any]:
        if self._query is None:
            return {"active": False}
        return {
            "id": self._query.id,
            "active": self._query.isActive,
            "status": self._query.status,
            "last_progress": self._query.lastProgress,
        }

    @property
    def is_active(self) -> bool:
        return self._query is not None and self._query.isActive
