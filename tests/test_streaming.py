"""Unit tests for StructuredStreamingProcessor and CDCProcessor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from src.ingestion.cdc_processor import CDCConfig, CDCEvent, CDCOperation, CDCProcessor
from src.ingestion.streaming_processor import StreamConfig, StructuredStreamingProcessor

# ── StreamConfig ──────────────────────────────────────────────────────────────


class TestStreamConfig:
    def test_default_values(self):
        cfg = StreamConfig(
            kafka_bootstrap_servers="localhost:9092",
            topics=["events"],
            checkpoint_location="gs://b/checkpoints",
            output_path="gs://b/bronze/events",
        )
        assert cfg.trigger_interval == "60 seconds"
        assert cfg.output_format == "delta"
        assert cfg.starting_offsets == "latest"
        assert cfg.max_offsets_per_trigger == 10_000
        assert cfg.watermark_delay == "10 minutes"

    def test_custom_values(self):
        cfg = StreamConfig(
            kafka_bootstrap_servers="broker:9092",
            topics=["t1", "t2"],
            checkpoint_location="gs://b/ckpt",
            output_path="gs://b/out",
            trigger_interval="30 seconds",
            output_format="iceberg",
        )
        assert cfg.trigger_interval == "30 seconds"
        assert cfg.output_format == "iceberg"
        assert len(cfg.topics) == 2

    def test_partition_columns_default(self):
        cfg = StreamConfig(
            kafka_bootstrap_servers="b:9092",
            topics=["t"],
            checkpoint_location="gs://b/c",
            output_path="gs://b/o",
        )
        assert cfg.partition_columns == ["_ingestion_date"]


# ── StructuredStreamingProcessor ──────────────────────────────────────────────


class TestStructuredStreamingProcessor:
    @pytest.fixture
    def config(self):
        return StreamConfig(
            kafka_bootstrap_servers="localhost:9092",
            topics=["orders", "events"],
            checkpoint_location="gs://bucket/checkpoints",
            output_path="gs://bucket/bronze",
        )

    def test_initial_state(self, mock_spark, config):
        proc = StructuredStreamingProcessor(mock_spark, config)
        assert proc._query is None
        assert not proc.is_active

    def test_status_before_start(self, mock_spark, config):
        proc = StructuredStreamingProcessor(mock_spark, config)
        assert proc.status == {"active": False}

    def test_stop_before_start_is_safe(self, mock_spark, config):
        proc = StructuredStreamingProcessor(mock_spark, config)
        proc.stop()  # should not raise

    def test_await_termination_before_start_returns_false(self, mock_spark, config):
        proc = StructuredStreamingProcessor(mock_spark, config)
        result = proc.await_termination(timeout_ms=100)
        assert result is False

    def test_enrich_calls_select_and_withcolumn(self, mock_spark, config):
        proc = StructuredStreamingProcessor(mock_spark, config)
        mock_df = MagicMock()
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.withWatermark.return_value = mock_df

        with patch("pyspark.sql.functions") as mock_f:
            mock_f.col.return_value = MagicMock()
            mock_f.current_timestamp.return_value = MagicMock()
            mock_f.to_date.return_value = MagicMock()
            mock_f.expr.return_value = MagicMock()
            proc._enrich(mock_df)

        mock_df.select.assert_called_once()
        assert mock_df.withColumn.call_count >= 3

    def test_status_after_start(self, mock_spark, config):
        mock_query = MagicMock()
        mock_query.id = "test-query-123"
        mock_query.isActive = True
        mock_query.status = {"message": "Processing"}
        mock_query.lastProgress = {}

        proc = StructuredStreamingProcessor(mock_spark, config)
        proc._query = mock_query

        status = proc.status
        assert status["id"] == "test-query-123"
        assert status["active"] is True

    def test_stop_calls_query_stop(self, mock_spark, config):
        mock_query = MagicMock()
        mock_query.isActive = True
        proc = StructuredStreamingProcessor(mock_spark, config)
        proc._query = mock_query
        proc.stop()
        mock_query.stop.assert_called_once()


# ── CDCEvent ──────────────────────────────────────────────────────────────────


class TestCDCEvent:
    def test_parse_create_event(self):
        raw = {
            "payload": {
                "op": "c",
                "before": None,
                "after": {"id": 1, "name": "Alice"},
                "source": {"table": "customers", "ts_ms": 1700000000000},
                "ts_ms": 1700000000000,
            }
        }
        event = CDCEvent.from_debezium(raw)
        assert event.op == CDCOperation.CREATE
        assert event.source_table == "customers"
        assert event.after == {"id": 1, "name": "Alice"}
        assert event.before is None

    def test_parse_delete_event(self):
        raw = {
            "payload": {
                "op": "d",
                "before": {"id": 2, "name": "Bob"},
                "after": None,
                "source": {"table": "orders"},
                "ts_ms": 1700000001000,
            }
        }
        event = CDCEvent.from_debezium(raw)
        assert event.op == CDCOperation.DELETE
        assert event.before == {"id": 2, "name": "Bob"}
        assert event.after is None

    def test_parse_update_event(self):
        raw = {
            "payload": {
                "op": "u",
                "before": {"status": "pending"},
                "after": {"status": "shipped"},
                "source": {"table": "orders"},
                "ts_ms": 1700000002000,
            }
        }
        event = CDCEvent.from_debezium(raw)
        assert event.op == CDCOperation.UPDATE
        assert event.after["status"] == "shipped"


# ── CDCProcessor ──────────────────────────────────────────────────────────────


class TestCDCProcessor:
    @pytest.fixture
    def config(self):
        return CDCConfig(
            kafka_bootstrap_servers="localhost:9092",
            debezium_topics=["dbserver1.public.customers"],
            bronze_output_path="gs://bucket/bronze/cdc",
            checkpoint_location="gs://bucket/checkpoints/cdc",
        )

    def test_init(self, config):
        proc = CDCProcessor(config)
        assert proc._consumer is None

    def test_process_empty_batch(self, config):
        proc = CDCProcessor(config)
        result = proc.process_batch([])
        assert result.total_events == 0

    def test_process_create_events(self, config):
        events = [
            CDCEvent(
                op=CDCOperation.CREATE,
                source_table="customers",
                ts_ms=1700000000000,
                after={"id": i, "name": f"User{i}"},
            )
            for i in range(5)
        ]
        result = CDCProcessor(config).process_batch(events)
        assert result.inserts == 5
        assert result.updates == 0
        assert result.deletes == 0

    def test_process_mixed_operations(self, config):
        events = [
            CDCEvent(op=CDCOperation.CREATE, source_table="orders", ts_ms=0, after={"id": 1}),
            CDCEvent(
                op=CDCOperation.UPDATE,
                source_table="orders",
                ts_ms=0,
                before={"id": 2, "status": "pending"},
                after={"id": 2, "status": "shipped"},
            ),
            CDCEvent(
                op=CDCOperation.DELETE,
                source_table="orders",
                ts_ms=0,
                before={"id": 3},
            ),
        ]
        result = CDCProcessor(config).process_batch(events)
        assert result.inserts == 1
        assert result.updates == 1
        assert result.deletes == 1
        assert result.total_events == 3

    def test_enrich_adds_cdc_columns(self, config):
        event = CDCEvent(op=CDCOperation.CREATE, source_table="orders", ts_ms=0, after={"id": 1})
        proc = CDCProcessor(config)
        enriched = proc._enrich({"id": 1}, event)
        assert "_cdc_op" in enriched
        assert "_cdc_ts_ms" in enriched
        assert "_cdc_source_table" in enriched
        assert "_cdc_deleted" in enriched
        assert "_ingestion_ts" in enriched
        assert enriched["_cdc_deleted"] is False

    def test_delete_sets_cdc_deleted_true(self, config):
        event = CDCEvent(op=CDCOperation.DELETE, source_table="orders", ts_ms=0, before={"id": 1})
        proc = CDCProcessor(config)
        enriched = proc._enrich({"id": 1}, event)
        assert enriched["_cdc_deleted"] is True

    def test_parse_invalid_event_returns_none(self, config):
        proc = CDCProcessor(config)
        result = proc.parse_event(b"not valid json {{{")
        assert result is None

    def test_close_without_consumer_is_safe(self, config):
        proc = CDCProcessor(config)
        proc.close()  # should not raise

    def test_result_summary(self, config):
        events = [CDCEvent(op=CDCOperation.CREATE, source_table="t", ts_ms=0, after={"id": 1})]
        result = CDCProcessor(config).process_batch(events)
        summary = result.summary()
        assert "t" in summary
        assert "+1" in summary
