"""Kafka event processor — consumes topics and writes to Bronze."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str = "earliest"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    batch_size: int = 1000
    batch_timeout_ms: int = 5000
    extra: dict[str, Any] = field(default_factory=dict)

    def to_consumer_config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "security.protocol": self.security_protocol,
            "enable.auto.commit": False,
        }
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
            cfg["sasl.username"] = self.sasl_username
            cfg["sasl.password"] = self.sasl_password
        cfg.update(self.extra)
        return cfg


class KafkaProcessor:
    """Consumes Kafka topics, deserialises records, and passes batches to a handler."""

    def __init__(self, config: KafkaConfig) -> None:
        self.config = config
        self._consumer: Consumer | None = None

    def _get_consumer(self) -> Consumer:
        if self._consumer is None:
            self._consumer = Consumer(self.config.to_consumer_config())
        return self._consumer

    def subscribe(self, topics: list[str]) -> None:
        self._get_consumer().subscribe(topics)
        logger.info("Subscribed to topics: %s", topics)

    def consume_batch(
        self,
        handler: Callable[[list[dict[str, Any]]], None],
        *,
        max_batches: int | None = None,
    ) -> None:
        """Pull records in batches of `batch_size`, call `handler` for each batch."""
        consumer = self._get_consumer()
        batch: list[dict[str, Any]] = []
        batches_processed = 0

        try:
            while True:
                msg = consumer.poll(timeout=self.config.batch_timeout_ms / 1000)

                if msg is None:
                    if batch:
                        handler(batch)
                        consumer.commit()
                        batches_processed += 1
                        batch = []
                    if max_batches and batches_processed >= max_batches:
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    record["_kafka_topic"] = msg.topic()
                    record["_kafka_partition"] = msg.partition()
                    record["_kafka_offset"] = msg.offset()
                    batch.append(record)
                except json.JSONDecodeError:
                    logger.warning("Skipping non-JSON message at offset %d", msg.offset())

                if len(batch) >= self.config.batch_size:
                    handler(batch)
                    consumer.commit()
                    batches_processed += 1
                    batch = []
                    if max_batches and batches_processed >= max_batches:
                        break
        finally:
            consumer.close()

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            self._consumer = None


class KafkaProducer:
    """Thin wrapper around confluent_kafka.Producer with structured logging."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        def _ack(err: Any, msg: Any) -> None:
            if err:
                logger.error("Delivery failed for topic %s: %s", topic, err)

        self._producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(value).encode(),
            callback=_ack,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)
