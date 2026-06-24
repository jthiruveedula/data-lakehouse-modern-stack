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
        gcp_project="test-project",
        gcs_bucket="test-bucket",
        bq_dataset="test_dataset",
        location="US",
        default_format=TableFormat.DELTA,
    )


@pytest.fixture
def orchestrator(config):
    with patch("src.lakehouse_manager.bigquery.Client"), \
         patch("src.lakehouse_manager.storage.Client"):
        return MedallionPipelineOrchestrator(config)


@pytest.fixture
def bronze_table():
    return LakehouseTable(
        name="orders",
        layer=MedallionLayer.BRONZE,
        table_format=TableFormat.DELTA,
        gcs_path="gs://test-bucket/bronze/orders",
        partition_cols=["_ingestion_date"],
        cluster_cols=["customer_id"],
        primary_keys=["order_id"],
        schema={"order_id": "string", "amount": "double"},
    )


@pytest.fixture
def silver_table():
    return LakehouseTable(
        name="orders",
        layer=MedallionLayer.SILVER,
        table_format=TableFormat.DELTA,
        gcs_path="gs://test-bucket/silver/orders",
        partition_cols=["_ingestion_date"],
        cluster_cols=["customer_id"],
        primary_keys=["order_id"],
        schema={"order_id": "string", "amount": "double"},
        scd_type=2,
    )


class TestMedallionPipelineOrchestrator:

    def test_init_creates_orchestrator(self, orchestrator, config):
        assert orchestrator.config.gcp_project == "test-project"
        assert orchestrator.config.gcs_bucket == "test-bucket"

    def test_ingest_to_bronze_delta(self, orchestrator, mock_df, bronze_table):
        mock_spark = MagicMock()
        enriched_df = MagicMock()
        enriched_df.count.return_value = 50
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 50

        with patch.object(orchestrator, "_enrich_bronze", return_value=mock_df):
            mock_df.write.format.return_value.mode.return_value \
                .partitionBy.return_value.save.return_value = None

            result = orchestrator.ingest_to_bronze(mock_df, bronze_table, "test_source")

        assert result["rows_written"] == 50
        assert "bronze/orders" in result["gcs_path"]

    def test_register_table(self, orchestrator, bronze_table):
        orchestrator.register_table(bronze_table)
        assert "orders" in orchestrator.table_registry

    def test_get_lineage_empty(self, orchestrator, bronze_table):
        orchestrator.register_table(bronze_table)
        lineage = orchestrator.get_lineage("orders")
        assert isinstance(lineage, dict)

    def test_scd2_merge_statement(self, orchestrator, silver_table):
        sql = orchestrator._build_scd2_merge(silver_table)
        assert "MERGE INTO" in sql
        assert "is_current" in sql.lower() or "_is_current" in sql.lower()
        assert "order_id" in sql

    def test_scd2_merge_contains_pk_join(self, orchestrator, silver_table):
        sql = orchestrator._build_scd2_merge(silver_table)
        assert "order_id" in sql

    def test_table_format_enum(self):
        assert TableFormat.DELTA.value == "delta"
        assert TableFormat.ICEBERG.value == "iceberg"

    def test_medallion_layer_enum(self):
        assert MedallionLayer.BRONZE.value == "bronze"
        assert MedallionLayer.SILVER.value == "silver"
        assert MedallionLayer.GOLD.value == "gold"


class TestLakehouseConfig:

    def test_default_format_is_delta(self, config):
        assert config.default_format == TableFormat.DELTA

    def test_config_fields(self, config):
        assert config.gcp_project == "test-project"
        assert config.gcs_bucket == "test-bucket"
        assert config.bq_dataset == "test_dataset"


class TestLakehouseTable:

    def test_table_creation(self, bronze_table):
        assert bronze_table.name == "orders"
        assert bronze_table.layer == MedallionLayer.BRONZE
        assert bronze_table.table_format == TableFormat.DELTA

    def test_scd_type_default_is_none(self):
        table = LakehouseTable(
            name="test",
            layer=MedallionLayer.SILVER,
            table_format=TableFormat.DELTA,
            gcs_path="gs://bucket/silver/test",
            partition_cols=[],
            cluster_cols=[],
            primary_keys=["id"],
            schema={},
        )
        assert table.scd_type is None
