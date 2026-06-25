"""Ingestion layer — Kafka, REST API, and CDC processors."""

from src.ingestion.api_ingester import APIIngester
from src.ingestion.kafka_processor import KafkaProcessor

__all__ = ["APIIngester", "KafkaProcessor"]
