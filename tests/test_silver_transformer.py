"""Unit tests for the Silver transformer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.silver.transformer import SilverTableConfig, SilverTransformer


@pytest.fixture
def transformer(mock_spark, sample_gcs_bucket):
    return SilverTransformer(spark=mock_spark, gcs_bucket=sample_gcs_bucket)


@pytest.fixture
def config():
    return SilverTableConfig(
        table_name="orders",
        primary_keys=["order_id"],
        scd2_enabled=False,
    )


class TestSilverTransformer:

    def test_init(self, transformer):
        assert transformer.gcs_bucket == "test-lakehouse-bucket"

    def test_transform_no_scd2(self, transformer, mock_df, config):
        with patch.object(transformer, "_deduplicate", return_value=mock_df), \
             patch.object(transformer, "_overwrite_silver"):
            result = transformer.transform(mock_df, config)

        assert result.table_name == "orders"
        assert result.rows_in == 100

    def test_transform_with_scd2(self, transformer, mock_df, config):
        config.scd2_enabled = True
        with patch.object(transformer, "_deduplicate", return_value=mock_df), \
             patch.object(transformer, "_apply_scd2", return_value=(80, 20)):
            result = transformer.transform(mock_df, config)

        assert result.scd2_inserts == 80
        assert result.scd2_updates == 20

    def test_duplicate_tracking(self, transformer, mock_df, config):
        deduped_df = MagicMock()
        deduped_df.count.return_value = 75
        with patch.object(transformer, "_deduplicate", return_value=deduped_df), \
             patch.object(transformer, "_overwrite_silver"):
            result = transformer.transform(mock_df, config)

        assert result.duplicates_dropped == 25  # 100 in - 75 deduped

    def test_rows_out_matches_deduped(self, transformer, mock_df, config):
        deduped_df = MagicMock()
        deduped_df.count.return_value = 90
        with patch.object(transformer, "_deduplicate", return_value=deduped_df), \
             patch.object(transformer, "_overwrite_silver"):
            result = transformer.transform(mock_df, config)

        assert result.rows_out == 90
