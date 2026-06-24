"""Shared pytest fixtures for the data lakehouse test suite."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_spark():
    """Minimal SparkSession mock — avoids JVM startup in unit tests."""
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.read = MagicMock()
    spark.read.format.return_value.option.return_value.load.return_value = MagicMock()
    return spark


@pytest.fixture
def sample_gcs_bucket():
    return "test-lakehouse-bucket"


@pytest.fixture
def sample_gcp_project():
    return "test-gcp-project"


@pytest.fixture
def sample_bq_dataset():
    return "test_dataset"


@pytest.fixture
def mock_df(mock_spark):
    """Mock Spark DataFrame with basic methods."""
    df = MagicMock()
    df.count.return_value = 100
    df.withColumn.return_value = df
    df.write = MagicMock()
    df.write.format.return_value = df.write
    df.write.mode.return_value = df.write
    df.write.partitionBy.return_value = df.write
    df.write.option.return_value = df.write
    df.write.save.return_value = None
    df.createOrReplaceTempView.return_value = None
    return df
