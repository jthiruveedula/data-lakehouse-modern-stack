"""Data Quality checker — null, unique, range, freshness, completeness, referential integrity."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class CheckSeverity(StrEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class CheckStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class CheckResult:
    check_name: str
    table: str
    column: str | None
    status: CheckStatus
    severity: CheckSeverity
    message: str
    actual_value: Any = None
    expected_value: Any = None
    checked_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    def as_dict(self) -> dict[str, Any]:
        return {
            "check": self.check_name,
            "table": self.table,
            "column": self.column,
            "status": self.status,
            "severity": self.severity,
            "message": self.message,
            "actual": self.actual_value,
            "expected": self.expected_value,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass
class QualityReport:
    table: str
    checks: list[CheckResult]
    run_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASS)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.FAIL)

    @property
    def is_healthy(self) -> bool:
        """True only if no ERROR-severity checks failed."""
        return all(
            c.status != CheckStatus.FAIL for c in self.checks if c.severity == CheckSeverity.ERROR
        )

    def summary(self) -> str:
        total = len(self.checks)
        health = "HEALTHY" if self.is_healthy else "UNHEALTHY"
        return f"[{self.table}] {self.passed}/{total} passed, {self.failed} failed — {health}"

    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == CheckStatus.FAIL]


class DataQualityChecker:
    """Fluent builder that runs DQ checks against a Spark table or view."""

    def __init__(self, spark: Any, table_name: str) -> None:
        self.spark = spark
        self.table_name = table_name
        self._checks: list[Callable[[], CheckResult]] = []

    # ── Builders ──────────────────────────────────────────────────────────────

    def check_not_null(
        self,
        column: str,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Assert a column contains no NULL values."""

        def _run() -> CheckResult:
            null_count = self.spark.sql(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL"
            ).collect()[0][0]
            status = CheckStatus.PASS if null_count == 0 else CheckStatus.FAIL
            msg = f"{null_count} null values found" if status == CheckStatus.FAIL else "no nulls"
            return CheckResult(
                check_name="not_null",
                table=self.table_name,
                column=column,
                status=status,
                severity=severity,
                message=msg,
                actual_value=null_count,
                expected_value=0,
            )

        self._checks.append(_run)
        return self

    def check_unique(
        self,
        column: str,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Assert a column has no duplicate non-null values."""

        def _run() -> CheckResult:
            dupes = self.spark.sql(
                f"SELECT COUNT(*) - COUNT(DISTINCT {column}) AS dupes FROM {self.table_name}"
            ).collect()[0][0]
            status = CheckStatus.PASS if dupes == 0 else CheckStatus.FAIL
            msg = f"{dupes} duplicate values" if status == CheckStatus.FAIL else "all unique"
            return CheckResult(
                check_name="unique",
                table=self.table_name,
                column=column,
                status=status,
                severity=severity,
                message=msg,
                actual_value=dupes,
                expected_value=0,
            )

        self._checks.append(_run)
        return self

    def check_range(
        self,
        column: str,
        min_val: float,
        max_val: float,
        severity: CheckSeverity = CheckSeverity.WARNING,
    ) -> DataQualityChecker:
        """Assert all numeric values fall within [min_val, max_val]."""

        def _run() -> CheckResult:
            row = self.spark.sql(
                f"SELECT MIN({column}), MAX({column}) FROM {self.table_name}"
            ).collect()[0]
            actual_min, actual_max = row[0], row[1]
            in_range = actual_min is None or (actual_min >= min_val and actual_max <= max_val)
            status = CheckStatus.PASS if in_range else CheckStatus.FAIL
            msg = (
                f"values outside [{min_val}, {max_val}]: min={actual_min}, max={actual_max}"
                if status == CheckStatus.FAIL
                else f"all values in [{min_val}, {max_val}]"
            )
            return CheckResult(
                check_name="range",
                table=self.table_name,
                column=column,
                status=status,
                severity=severity,
                message=msg,
                actual_value={"min": actual_min, "max": actual_max},
                expected_value={"min": min_val, "max": max_val},
            )

        self._checks.append(_run)
        return self

    def check_freshness(
        self,
        ts_column: str,
        max_age: timedelta,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Assert the most recent record timestamp is within max_age of now."""

        def _run() -> CheckResult:
            latest = self.spark.sql(f"SELECT MAX({ts_column}) FROM {self.table_name}").collect()[0][
                0
            ]
            if latest is None:
                return CheckResult(
                    check_name="freshness",
                    table=self.table_name,
                    column=ts_column,
                    status=CheckStatus.FAIL,
                    severity=severity,
                    message="table has no rows",
                )
            age = datetime.now(tz=UTC) - latest.replace(tzinfo=UTC)
            status = CheckStatus.PASS if age <= max_age else CheckStatus.FAIL
            return CheckResult(
                check_name="freshness",
                table=self.table_name,
                column=ts_column,
                status=status,
                severity=severity,
                message=f"data is {age} old (max: {max_age})",
                actual_value=str(age),
                expected_value=str(max_age),
            )

        self._checks.append(_run)
        return self

    def check_row_count(
        self,
        min_rows: int,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Assert the table has at least min_rows rows."""

        def _run() -> CheckResult:
            count = self.spark.sql(f"SELECT COUNT(*) FROM {self.table_name}").collect()[0][0]
            status = CheckStatus.PASS if count >= min_rows else CheckStatus.FAIL
            return CheckResult(
                check_name="row_count",
                table=self.table_name,
                column=None,
                status=status,
                severity=severity,
                message=f"{count} rows (min required: {min_rows})",
                actual_value=count,
                expected_value=min_rows,
            )

        self._checks.append(_run)
        return self

    def check_completeness(
        self,
        columns: list[str],
        min_pct: float = 0.95,
        severity: CheckSeverity = CheckSeverity.WARNING,
    ) -> DataQualityChecker:
        """Assert the fraction of fully-populated rows meets min_pct."""

        def _run() -> CheckResult:
            null_conditions = " OR ".join(f"{c} IS NULL" for c in columns)
            row = self.spark.sql(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN {null_conditions} THEN 0 ELSE 1 END) AS complete
                FROM {self.table_name}
                """
            ).collect()[0]
            total, complete = row[0], row[1]
            pct = complete / total if total > 0 else 0.0
            status = CheckStatus.PASS if pct >= min_pct else CheckStatus.FAIL
            return CheckResult(
                check_name="completeness",
                table=self.table_name,
                column=", ".join(columns),
                status=status,
                severity=severity,
                message=f"{pct:.1%} complete (min: {min_pct:.1%})",
                actual_value=round(pct, 4),
                expected_value=min_pct,
            )

        self._checks.append(_run)
        return self

    def check_referential_integrity(
        self,
        column: str,
        ref_table: str,
        ref_column: str,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Assert every non-null value in column exists in ref_table.ref_column."""

        def _run() -> CheckResult:
            orphans = self.spark.sql(
                f"""
                SELECT COUNT(*) FROM {self.table_name} t
                LEFT JOIN {ref_table} r ON t.{column} = r.{ref_column}
                WHERE r.{ref_column} IS NULL AND t.{column} IS NOT NULL
                """
            ).collect()[0][0]
            status = CheckStatus.PASS if orphans == 0 else CheckStatus.FAIL
            return CheckResult(
                check_name="referential_integrity",
                table=self.table_name,
                column=column,
                status=status,
                severity=severity,
                message=f"{orphans} orphan rows (no match in {ref_table}.{ref_column})",
                actual_value=orphans,
                expected_value=0,
            )

        self._checks.append(_run)
        return self

    def check_accepted_values(
        self,
        column: str,
        accepted: list[str],
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Assert all values in column belong to an accepted set."""

        def _run() -> CheckResult:
            quoted = ", ".join(f"'{v}'" for v in accepted)
            invalid = self.spark.sql(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} NOT IN ({quoted})"
            ).collect()[0][0]
            status = CheckStatus.PASS if invalid == 0 else CheckStatus.FAIL
            return CheckResult(
                check_name="accepted_values",
                table=self.table_name,
                column=column,
                status=status,
                severity=severity,
                message=f"{invalid} rows with unexpected values",
                actual_value=invalid,
                expected_value=0,
            )

        self._checks.append(_run)
        return self

    # ── Execution ──────────────────────────────────────────────────────────────

    def run(self) -> QualityReport:
        """Execute all registered checks and return a consolidated report."""
        results: list[CheckResult] = []
        for check_fn in self._checks:
            try:
                result = check_fn()
                results.append(result)
                icon = "✓" if result.status == CheckStatus.PASS else "✗"
                logger.info(
                    "%s [%s.%s] %s — %s",
                    icon,
                    result.table,
                    result.column or "*",
                    result.check_name,
                    result.message,
                )
            except Exception as exc:
                logger.error("Check raised exception: %s", exc, exc_info=True)
                results.append(
                    CheckResult(
                        check_name="error",
                        table=self.table_name,
                        column=None,
                        status=CheckStatus.FAIL,
                        severity=CheckSeverity.ERROR,
                        message=f"check raised exception: {exc}",
                    )
                )
        report = QualityReport(table=self.table_name, checks=results)
        logger.info(report.summary())
        return report
