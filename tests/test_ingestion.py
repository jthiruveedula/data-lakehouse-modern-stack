"""Unit tests for the ingestion layer (APIIngester, KafkaProcessor)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from src.ingestion.api_ingester import APIConfig, APIIngester
from src.ingestion.kafka_processor import KafkaConfig, KafkaProcessor


class TestAPIIngester:
    @pytest.fixture
    def config(self):
        return APIConfig(
            base_url="https://api.example.com",
            endpoint="/v1/orders",
            page_size=10,
            results_key="results",
        )

    @pytest.fixture
    def ingester(self, config):
        return APIIngester(config)

    def test_init(self, ingester, config):
        assert ingester.config.base_url == "https://api.example.com"
        assert ingester.config.page_size == 10

    def test_paginate_single_page(self, ingester):
        page1 = [{"id": i} for i in range(5)]  # fewer than page_size → last page

        with patch.object(ingester, "_fetch_page", return_value={"results": page1}):
            pages = list(ingester.paginate())

        assert len(pages) == 1
        assert pages[0] == page1

    def test_paginate_multiple_pages(self, ingester):
        full_page = [{"id": i} for i in range(10)]
        last_page = [{"id": i} for i in range(3)]

        call_count = [0]

        def fake_fetch(params):
            call_count[0] += 1
            return {"results": full_page if call_count[0] == 1 else last_page}

        with patch.object(ingester, "_fetch_page", side_effect=fake_fetch):
            pages = list(ingester.paginate())

        assert len(pages) == 2
        assert pages[0] == full_page
        assert pages[1] == last_page

    def test_ingest_all(self, ingester):
        records = [{"id": i} for i in range(5)]
        with patch.object(ingester, "_fetch_page", return_value={"results": records}):
            all_records = ingester.ingest_all()
        assert len(all_records) == 5

    def test_empty_response(self, ingester):
        with patch.object(ingester, "_fetch_page", return_value={"results": []}):
            pages = list(ingester.paginate())
        assert pages == []

    def test_auth_header_set(self):
        config = APIConfig(
            base_url="https://api.example.com",
            endpoint="/v1/data",
            auth_header="Bearer my-token",
        )
        ing = APIIngester(config)
        assert ing._session.headers.get("Authorization") == "Bearer my-token"


class TestKafkaConfig:
    def test_consumer_config_basic(self):
        cfg = KafkaConfig(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        consumer_cfg = cfg.to_consumer_config()
        assert consumer_cfg["bootstrap.servers"] == "localhost:9092"
        assert consumer_cfg["group.id"] == "test-group"
        assert consumer_cfg["enable.auto.commit"] is False

    def test_consumer_config_with_sasl(self):
        cfg = KafkaConfig(
            bootstrap_servers="broker:9093",
            group_id="grp",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
        )
        consumer_cfg = cfg.to_consumer_config()
        assert consumer_cfg["sasl.mechanism"] == "PLAIN"
        assert consumer_cfg["sasl.username"] == "user"
        assert consumer_cfg["sasl.password"] == "pass"


class TestKafkaProcessor:
    @pytest.fixture
    def cfg(self):
        return KafkaConfig(bootstrap_servers="localhost:9092", group_id="test")

    def test_init(self, cfg):
        proc = KafkaProcessor(cfg)
        assert proc.config.bootstrap_servers == "localhost:9092"
        assert proc._consumer is None

    def test_close_without_init(self, cfg):
        proc = KafkaProcessor(cfg)
        proc.close()  # should not raise

    def test_subscribe_creates_consumer(self, cfg):
        with patch("src.ingestion.kafka_processor.Consumer") as MockConsumer:
            mock_consumer = MagicMock()
            MockConsumer.return_value = mock_consumer
            proc = KafkaProcessor(cfg)
            proc.subscribe(["topic1"])
            mock_consumer.subscribe.assert_called_once_with(["topic1"])
