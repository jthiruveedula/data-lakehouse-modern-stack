"""Cloud Monitoring custom metrics for the lakehouse pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

METRIC_PREFIX = "custom.googleapis.com/lakehouse"


@dataclass
class MetricPoint:
    metric_type: str
    value: float
    labels: dict[str, str]
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(tz=UTC)


class LakehouseMetrics:
    """Writes custom time-series metrics to Google Cloud Monitoring."""

    def __init__(self, project_id: str, *, dry_run: bool = False) -> None:
        self.project_id = project_id
        self.dry_run = dry_run
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            from google.cloud import monitoring_v3

            self._client = monitoring_v3.MetricServiceClient()
        return self._client

    def _write(self, points: list[MetricPoint]) -> None:
        if self.dry_run:
            for p in points:
                logger.info("[dry-run] metric %s = %s  labels=%s", p.metric_type, p.value, p.labels)
            return

        from google.cloud import monitoring_v3
        from google.protobuf import timestamp_pb2

        client = self._get_client()
        project_name = f"projects/{self.project_id}"

        time_series_list = []
        for point in points:
            series = monitoring_v3.TimeSeries()
            series.metric.type = f"{METRIC_PREFIX}/{point.metric_type}"
            for k, v in point.labels.items():
                series.metric.labels[k] = v
            series.resource.type = "global"
            series.resource.labels["project_id"] = self.project_id

            interval = monitoring_v3.TimeInterval()
            ts = timestamp_pb2.Timestamp()
            ts.FromDatetime(point.timestamp)
            interval.end_time = ts

            data_point = monitoring_v3.Point()
            data_point.interval = interval
            data_point.value.double_value = point.value
            series.points.append(data_point)
            time_series_list.append(series)

        client.create_time_series(name=project_name, time_series=time_series_list)
        logger.info("Wrote %d metric points to Cloud Monitoring", len(points))

    # ── High-level helpers ────────────────────────────────────────────────────

    def record_ingestion(
        self,
        table: str,
        layer: str,
        rows_written: int,
        duration_seconds: float,
        source: str = "unknown",
    ) -> None:
        """Record rows ingested and throughput for a Bronze/Silver/Gold write."""
        labels = {"table": table, "layer": layer, "source": source}
        self._write(
            [
                MetricPoint("ingestion/rows_written", rows_written, labels),
                MetricPoint("ingestion/duration_seconds", duration_seconds, labels),
                MetricPoint(
                    "ingestion/rows_per_second",
                    rows_written / max(duration_seconds, 0.001),
                    labels,
                ),
            ]
        )

    def record_quality_check(
        self,
        table: str,
        checks_passed: int,
        checks_failed: int,
    ) -> None:
        """Record DQ check results for a table."""
        labels = {"table": table}
        total = checks_passed + checks_failed
        pass_rate = checks_passed / total if total > 0 else 1.0
        self._write(
            [
                MetricPoint("quality/checks_passed", checks_passed, labels),
                MetricPoint("quality/checks_failed", checks_failed, labels),
                MetricPoint("quality/pass_rate", pass_rate, labels),
            ]
        )

    def record_pipeline_run(
        self,
        pipeline: str,
        status: str,
        duration_seconds: float,
        rows_processed: int = 0,
    ) -> None:
        """Record an end-to-end pipeline execution result."""
        labels = {"pipeline": pipeline, "status": status}
        self._write(
            [
                MetricPoint("pipeline/duration_seconds", duration_seconds, labels),
                MetricPoint("pipeline/rows_processed", rows_processed, labels),
                MetricPoint("pipeline/success", 1.0 if status == "success" else 0.0, labels),
            ]
        )

    def record_table_size(
        self,
        table: str,
        layer: str,
        row_count: int,
        size_bytes: int,
    ) -> None:
        """Record table row count and storage size."""
        labels = {"table": table, "layer": layer}
        self._write(
            [
                MetricPoint("table/row_count", row_count, labels),
                MetricPoint("table/size_bytes", size_bytes, labels),
            ]
        )

    def record_query_latency(
        self,
        query_type: str,
        duration_ms: float,
        success: bool = True,
    ) -> None:
        """Record semantic search / Trino query latency."""
        labels = {"query_type": query_type, "success": str(success).lower()}
        self._write([MetricPoint("query/latency_ms", duration_ms, labels)])
