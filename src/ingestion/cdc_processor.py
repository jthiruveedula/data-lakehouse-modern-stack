"""Change Data Capture (CDC) processor — applies Debezium-format events to Bronze."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class CDCOperation(StrEnum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # snapshot / initial load


@dataclass
class CDCEvent:
    """Represents a single Debezium change event."""

    op: CDCOperation
    source_table: str
    ts_ms: int
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    transaction_id: str | None = None

    @classmethod
    def from_debezium(cls, raw: dict[str, Any]) -> CDCEvent:
        """Parse a Debezium envelope into a CDCEvent."""
        payload = raw.get("payload", raw)
        source = payload.get("source", {})
        return cls(
            op=CDCOperation(payload["op"]),
            source_table=source.get("table", "unknown"),
            ts_ms=payload.get("ts_ms", 0),
            before=payload.get("before"),
            after=payload.get("after"),
            transaction_id=payload.get("transaction", {}).get("id"),
        )


@dataclass
class CDCResult:
    source_table: str
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    errors: int = 0

    @property
    def total_events(self) -> int:
        return self.inserts + self.updates + self.deletes

    def summary(self) -> str:
        return (
            f"[{self.source_table}] "
            f"+{self.inserts} ~{self.updates} -{self.deletes} "
            f"({self.errors} errors)"
        )


@dataclass
class CDCConfig:
    kafka_bootstrap_servers: str
    debezium_topics: list[str]
    bronze_output_path: str
    checkpoint_location: str
    group_id: str = "cdc-lakehouse-consumer"
    dead_letter_path: str | None = None
    batch_size: int = 5_000
    extra_kafka_config: dict[str, str] = field(default_factory=dict)


class CDCProcessor:
    """Reads Debezium CDC events from Kafka and applies them to Bronze Delta tables.

    Supports create/update/delete operations with soft deletes (_cdc_deleted flag)
    and full audit trail via _cdc_op and _cdc_ts columns.
    """

    CDC_COLUMNS = ("_cdc_op", "_cdc_ts_ms", "_cdc_source_table", "_cdc_tx_id", "_ingestion_ts")

    def __init__(self, config: CDCConfig) -> None:
        self.config = config
        self._consumer: Any = None

    def _get_consumer(self) -> Any:
        from confluent_kafka import Consumer

        cfg = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "group.id": self.config.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            **self.config.extra_kafka_config,
        }
        consumer = Consumer(cfg)
        consumer.subscribe(self.config.debezium_topics)
        return consumer

    def parse_event(self, raw_bytes: bytes) -> CDCEvent | None:
        try:
            raw = json.loads(raw_bytes.decode("utf-8"))
            return CDCEvent.from_debezium(raw)
        except Exception as exc:
            logger.warning("Failed to parse CDC event: %s", exc)
            return None

    def _enrich(self, record: dict[str, Any], event: CDCEvent) -> dict[str, Any]:
        from datetime import datetime

        return {
            **record,
            "_cdc_op": event.op,
            "_cdc_ts_ms": event.ts_ms,
            "_cdc_source_table": event.source_table,
            "_cdc_tx_id": event.transaction_id,
            "_ingestion_ts": datetime.now(tz=UTC).isoformat(),
            "_cdc_deleted": event.op == CDCOperation.DELETE,
        }

    def process_batch(self, events: list[CDCEvent]) -> CDCResult:
        """Categorise and enrich a batch of CDC events.

        Returns a CDCResult with counts; the enriched rows are logged here
        but would normally be written to a Delta/Iceberg Bronze table by
        the caller (or a Spark Structured Streaming write).
        """
        if not events:
            return CDCResult(source_table="none")

        source_table = events[0].source_table
        result = CDCResult(source_table=source_table)
        rows: list[dict[str, Any]] = []

        for event in events:
            try:
                if event.op in (CDCOperation.CREATE, CDCOperation.READ) and event.after:
                    rows.append(self._enrich(event.after, event))
                    result.inserts += 1
                elif event.op == CDCOperation.UPDATE and event.after:
                    rows.append(self._enrich(event.after, event))
                    result.updates += 1
                elif event.op == CDCOperation.DELETE and event.before:
                    # Soft delete: write the before-image with _cdc_deleted=True
                    rows.append(self._enrich(event.before, event))
                    result.deletes += 1
            except Exception as exc:
                logger.warning("Failed to process event op=%s: %s", event.op, exc)
                result.errors += 1

        logger.info(result.summary())
        return result

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
