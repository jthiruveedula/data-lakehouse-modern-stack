"""Column-level statistical profiler — distributions, cardinality, null rates."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    count: int
    null_count: int
    null_pct: float
    distinct_count: int
    distinct_pct: float
    min_value: Any = None
    max_value: Any = None
    mean: float | None = None
    stddev: float | None = None
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    top_values: list[tuple[Any, int]] | None = None

    @property
    def is_high_cardinality(self) -> bool:
        return self.distinct_pct > 0.9

    @property
    def is_constant(self) -> bool:
        return self.distinct_count <= 1


@dataclass
class TableProfile:
    table_name: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile]

    def to_markdown(self) -> str:
        lines = [
            f"## Profile: `{self.table_name}`",
            f"**Rows**: {self.row_count:,}  |  **Columns**: {self.column_count}",
            "",
            "| Column | Type | Nulls | Distinct | Min | Max |",
            "|--------|------|-------|----------|-----|-----|",
        ]
        for col in self.columns:
            lines.append(
                f"| {col.name} | {col.dtype} | {col.null_pct:.1%} | "
                f"{col.distinct_pct:.1%} | {col.min_value} | {col.max_value} |"
            )
        return "\n".join(lines)


class TableProfiler:
    """Computes statistical profiles for all columns in a Spark table."""

    NUMERIC_TYPES = {"int", "bigint", "double", "float", "decimal", "long"}

    def __init__(self, spark: Any, table_name: str, top_n: int = 5) -> None:
        self.spark = spark
        self.table_name = table_name
        self.top_n = top_n

    def profile(self) -> TableProfile:
        df = self.spark.table(self.table_name)
        row_count = df.count()
        schema = df.schema
        df.createOrReplaceTempView("_profile_tmp")

        columns = []
        for field in schema:
            col_profile = self._profile_column(field.name, str(field.dataType), row_count)
            columns.append(col_profile)
            logger.debug("Profiled column: %s", field.name)

        return TableProfile(
            table_name=self.table_name,
            row_count=row_count,
            column_count=len(columns),
            columns=columns,
        )

    def _profile_column(self, col: str, dtype: str, total_rows: int) -> ColumnProfile:
        base = self.spark.sql(
            f"""
            SELECT
                COUNT(*) AS cnt,
                COUNT({col}) AS non_null_cnt,
                COUNT(DISTINCT {col}) AS distinct_cnt
            FROM _profile_tmp
            """
        ).collect()[0]

        null_count = total_rows - base[1]
        null_pct = null_count / total_rows if total_rows > 0 else 0.0
        distinct_pct = base[2] / total_rows if total_rows > 0 else 0.0

        min_val = max_val = mean = stddev = p25 = p50 = p75 = None
        dtype_lower = dtype.lower().split("(")[0].strip()

        if dtype_lower in self.NUMERIC_TYPES:
            stats = self.spark.sql(
                f"""
                SELECT
                    MIN({col}), MAX({col}),
                    AVG({col}),
                    STDDEV({col}),
                    PERCENTILE_APPROX({col}, 0.25),
                    PERCENTILE_APPROX({col}, 0.50),
                    PERCENTILE_APPROX({col}, 0.75)
                FROM _profile_tmp
                """
            ).collect()[0]
            min_val, max_val, mean, stddev, p25, p50, p75 = stats
        else:
            minmax = self.spark.sql(
                f"SELECT MIN(CAST({col} AS STRING)), MAX(CAST({col} AS STRING)) FROM _profile_tmp"
            ).collect()[0]
            min_val, max_val = minmax

        top_values = None
        if not (dtype_lower in self.NUMERIC_TYPES and distinct_pct > 0.5):
            rows = self.spark.sql(
                f"""
                SELECT {col}, COUNT(*) AS cnt
                FROM _profile_tmp
                WHERE {col} IS NOT NULL
                GROUP BY {col}
                ORDER BY cnt DESC
                LIMIT {self.top_n}
                """
            ).collect()
            top_values = [(r[0], r[1]) for r in rows]

        return ColumnProfile(
            name=col,
            dtype=dtype,
            count=total_rows,
            null_count=null_count,
            null_pct=null_pct,
            distinct_count=base[2],
            distinct_pct=distinct_pct,
            min_value=min_val,
            max_value=max_val,
            mean=mean,
            stddev=stddev,
            p25=p25,
            p50=p50,
            p75=p75,
            top_values=top_values,
        )
