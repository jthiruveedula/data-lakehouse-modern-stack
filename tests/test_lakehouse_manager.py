"""Unit tests for the core MedallionPipelineOrchestrator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from src.lakehouse_manager import (
    LakehouseConfig,
    LakehouseTable,
    MedallionLayer,
    MedallionPipelineOrchestrator,
    TableFormat,
)


@pytest.fixture
def config():
    return LakehouseConfig(
        project_id="test-project",
        gcs_bucket="test-bucket",
        bq_dataset="test_dataset",
        location="US",
        default_format=TableFormat.DELTA,
    )


@pytest.fixture
def orchestrator(config):
    with (
        patch("src.lakehouse_manager.bigquery.Client"),
        patch("src.lakehouse_manager.storage.Client"),
    ):
        return MedallionPipelineOrchestrator(config)


@pytest.fixture
def bronze_table():
    return LakehouseTable(
        name="orders",
        layer=MedallionLayer.BRONZE,
        format=TableFormat.DELTA,
        gcs_path="gs://test-bucket/bronze/orders",
        partition_columns=["_ingestion_date"],
        cluster_columns=["customer_id"],
        primary_keys=["order_id"],
        schema=[{"name": "order_id", "type": "STRING"}, {"name": "amount", "type": "FLOAT64"}],
    )


@pytest.fixture
def silver_table():
    return LakehouseTable(
        name="orders",
        layer=MedallionLayer.SILVER,
        format=TableFormat.DELTA,
        gcs_path="gs://test-bucket/silver/orders",
        partition_columns=["_ingestion_date"],
        cluster_columns=["customer_id"],
        primary_keys=["order_id"],
        schema=[{"name": "order_id", "type": "STRING"}, {"name": "amount", "type": "FLOAT64"}],
        scd_type=2,
    )


class TestMedallionPipelineOrchestrator:
    def test_init_creates_orchestrator(self, orchestrator, config):
        assert orchestrator.config.project_id == "test-project"
        assert orchestrator.config.gcs_bucket == "test-bucket"

    def test_ingest_to_bronze(self, orchestrator, bronze_table):
        orchestrator.register_table(bronze_table)
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        orchestrator.gcs.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        records = [{"order_id": "1", "amount": 9.99}]
        result = orchestrator.ingest_to_bronze("orders", records, "test_source")

        assert result["rows"] == 1
        assert "bronze" in result["layer"]
        mock_blob.upload_from_string.assert_called_once()

    def test_register_table(self, orchestrator, bronze_table):
        orchestrator.register_table(bronze_table)
        assert "orders" in orchestrator._tables

    def test_get_lineage_returns_dict(self, orchestrator, bronze_table):
        orchestrator.register_table(bronze_table)
        lineage = orchestrator.get_lineage("orders")
        assert isinstance(lineage, dict)
        assert "lineage" in lineage

    def test_scd2_merge_statement_contains_merge(self, orchestrator, silver_table):
        sql = orchestrator._build_scd2_merge(silver_table, "project.dataset.orders_bronze")
        assert "MERGE" in sql
        assert "is_current" in sql.lower()

    def test_scd2_merge_contains_pk_join(self, orchestrator, silver_table):
        sql = orchestrator._build_scd2_merge(silver_table, "project.dataset.orders_bronze")
        assert "order_id" in sql

    def test_scd2_merge_source_ref(self, orchestrator, silver_table):
        source_ref = "project.dataset.orders_bronze"
        sql = orchestrator._build_scd2_merge(silver_table, source_ref)
        assert source_ref in sql

    def test_table_format_enum(self):
        assert TableFormat.DELTA == "delta"
        assert TableFormat.ICEBERG == "iceberg"

    def test_medallion_layer_enum(self):
        assert MedallionLayer.BRONZE == "bronze"
        assert MedallionLayer.SILVER == "silver"
        assert MedallionLayer.GOLD == "gold"


class TestLakehouseConfig:
    def test_default_format_is_delta(self, config):
        assert config.default_format == TableFormat.DELTA

    def test_config_fields(self, config):
        assert config.project_id == "test-project"
        assert config.gcs_bucket == "test-bucket"
        assert config.bq_dataset == "test_dataset"

    def test_default_format_fallback(self):
        cfg = LakehouseConfig(project_id="p", gcs_bucket="b")
        assert cfg.default_format == TableFormat.ICEBERG


class TestLakehouseTable:
    def test_table_creation(self, bronze_table):
        assert bronze_table.name == "orders"
        assert bronze_table.layer == MedallionLayer.BRONZE
        assert bronze_table.format == TableFormat.DELTA

    def test_scd_type_default_is_one(self):
        table = LakehouseTable(
            name="test",
            layer=MedallionLayer.SILVER,
            format=TableFormat.DELTA,
            gcs_path="gs://bucket/silver/test",
        )
        assert table.scd_type == 1

    def test_scd_type_override(self, silver_table):
        assert silver_table.scd_type == 2

    def test_primary_keys_default_empty(self):
        table = LakehouseTable(
            name="test",
            layer=MedallionLayer.BRONZE,
            format=TableFormat.DELTA,
            gcs_path="gs://bucket/bronze/test",
        )
        assert table.primary_keys == []
