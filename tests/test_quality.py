"""Unit tests for the DataQualityChecker."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from src.quality.checker import (
    CheckSeverity,
    CheckStatus,
    DataQualityChecker,
)


@pytest.fixture
def checker(mock_spark):
    return DataQualityChecker(mock_spark, "test_table")


def _sql_returns(mock_spark, value):
    """Configure mock_spark.sql().collect() to return [[value]]."""
    mock_spark.sql.return_value.collect.return_value = [[value]]


def _sql_returns_row(mock_spark, *values):
    """Configure mock_spark.sql().collect() to return [list(values)]."""
    mock_spark.sql.return_value.collect.return_value = [list(values)]


class TestCheckNotNull:
    def test_passes_when_no_nulls(self, mock_spark, checker):
        _sql_returns(mock_spark, 0)
        report = checker.check_not_null("id").run()
        assert report.passed == 1
        assert report.is_healthy

    def test_fails_when_nulls_exist(self, mock_spark, checker):
        _sql_returns(mock_spark, 7)
        report = checker.check_not_null("id").run()
        assert report.failed == 1
        assert not report.is_healthy

    def test_warning_severity_does_not_block_health(self, mock_spark, checker):
        _sql_returns(mock_spark, 3)
        report = checker.check_not_null("optional_col", severity=CheckSeverity.WARNING).run()
        assert report.failed == 1
        assert report.is_healthy  # warning failures don't make is_healthy=False


class TestCheckUnique:
    def test_passes_with_no_dupes(self, mock_spark, checker):
        _sql_returns(mock_spark, 0)
        report = checker.check_unique("order_id").run()
        assert report.passed == 1

    def test_fails_with_dupes(self, mock_spark, checker):
        _sql_returns(mock_spark, 5)
        report = checker.check_unique("order_id").run()
        assert report.failed == 1
        result = report.checks[0]
        assert result.actual_value == 5
        assert "duplicate" in result.message


class TestCheckRange:
    def test_passes_within_range(self, mock_spark, checker):
        _sql_returns_row(mock_spark, 0.0, 100.0)
        report = checker.check_range("amount", 0, 1000).run()
        assert report.passed == 1

    def test_fails_outside_range(self, mock_spark, checker):
        _sql_returns_row(mock_spark, -10.0, 200.0)
        report = checker.check_range("amount", 0, 100).run()
        assert report.failed == 1

    def test_passes_with_none_min(self, mock_spark, checker):
        _sql_returns_row(mock_spark, None, None)
        report = checker.check_range("amount", 0, 100).run()
        assert report.passed == 1  # NULL min treated as empty table → pass


class TestCheckRowCount:
    def test_passes_above_minimum(self, mock_spark, checker):
        _sql_returns(mock_spark, 1000)
        report = checker.check_row_count(min_rows=100).run()
        assert report.passed == 1

    def test_fails_below_minimum(self, mock_spark, checker):
        _sql_returns(mock_spark, 50)
        report = checker.check_row_count(min_rows=100).run()
        assert report.failed == 1
        result = report.checks[0]
        assert result.actual_value == 50
        assert result.expected_value == 100

    def test_exact_minimum_passes(self, mock_spark, checker):
        _sql_returns(mock_spark, 100)
        report = checker.check_row_count(min_rows=100).run()
        assert report.passed == 1


class TestCheckAcceptedValues:
    def test_passes_all_valid(self, mock_spark, checker):
        _sql_returns(mock_spark, 0)
        report = checker.check_accepted_values("status", ["active", "inactive"]).run()
        assert report.passed == 1

    def test_fails_with_invalid_values(self, mock_spark, checker):
        _sql_returns(mock_spark, 3)
        report = checker.check_accepted_values("status", ["active", "inactive"]).run()
        assert report.failed == 1


class TestFluentChaining:
    def test_multiple_checks(self, mock_spark, checker):
        _sql_returns(mock_spark, 0)
        report = (checker.check_not_null("id").check_unique("id").check_row_count(min_rows=1)).run()
        assert len(report.checks) == 3

    def test_mixed_pass_fail(self, mock_spark, checker):
        results = [[[0]], [[5]], [[1000]]]
        call_count = [0]

        def side_effect(sql):
            m = MagicMock()
            m.collect.return_value = results[call_count[0]]
            call_count[0] += 1
            return m

        mock_spark.sql.side_effect = side_effect
        report = (
            DataQualityChecker(mock_spark, "test_table")
            .check_not_null("id")
            .check_unique("id")
            .check_row_count(min_rows=1)
        ).run()
        assert report.passed == 2
        assert report.failed == 1


class TestQualityReport:
    def test_summary_healthy(self, mock_spark):
        _sql_returns(mock_spark, 0)
        report = DataQualityChecker(mock_spark, "orders").check_not_null("id").run()
        assert "HEALTHY" in report.summary()
        assert "orders" in report.summary()

    def test_summary_unhealthy(self, mock_spark):
        _sql_returns(mock_spark, 10)
        report = DataQualityChecker(mock_spark, "orders").check_not_null("id").run()
        assert "UNHEALTHY" in report.summary()

    def test_failed_checks_list(self, mock_spark):
        _sql_returns(mock_spark, 5)
        report = DataQualityChecker(mock_spark, "orders").check_not_null("id").run()
        failed = report.failed_checks()
        assert len(failed) == 1
        assert failed[0].status == CheckStatus.FAIL

    def test_as_dict(self, mock_spark):
        _sql_returns(mock_spark, 0)
        report = DataQualityChecker(mock_spark, "orders").check_not_null("id").run()
        d = report.checks[0].as_dict()
        assert d["check"] == "not_null"
        assert d["status"] == "pass"
        assert "checked_at" in d

    def test_exception_in_check_captured(self, mock_spark):
        mock_spark.sql.side_effect = RuntimeError("Spark error")
        report = DataQualityChecker(mock_spark, "orders").check_not_null("id").run()
        assert report.failed == 1
        assert "Spark error" in report.checks[0].message
