"""Cost analysis and optimization recommendations for BigQuery and Dataproc."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class QueryCostSummary:
    query_hash: str
    query_text: str
    execution_count: int
    avg_bytes_processed: int
    total_bytes_processed: int
    avg_slot_ms: int
    estimated_cost_usd: float
    recommendation: str | None = None


@dataclass
class TableCostSummary:
    table_id: str
    size_gb: float
    active_logical_gb: float
    long_term_logical_gb: float
    monthly_storage_cost_usd: float
    last_modified: date | None = None
    recommendation: str | None = None


@dataclass
class CostReport:
    project_id: str
    report_date: date
    query_summaries: list[QueryCostSummary]
    table_summaries: list[TableCostSummary]
    total_estimated_monthly_cost_usd: float
    potential_savings_usd: float
    top_recommendations: list[str]


# BigQuery on-demand pricing (USD per TB processed)
BQ_PRICE_PER_TB = 5.00
BQ_ACTIVE_STORAGE_PER_GB_MONTH = 0.02
BQ_LONG_TERM_STORAGE_PER_GB_MONTH = 0.01


class BigQueryCostOptimizer:
    """Analyses BigQuery usage to surface cost reduction opportunities."""

    def __init__(self, project_id: str, bq_client: Any | None = None) -> None:
        self.project_id = project_id
        self._bq = bq_client

    def _get_client(self) -> Any:
        if self._bq is None:
            from google.cloud import bigquery

            self._bq = bigquery.Client(project=self.project_id)
        return self._bq

    def analyse_expensive_queries(
        self,
        lookback_days: int = 30,
        top_n: int = 20,
    ) -> list[QueryCostSummary]:
        """Return the top-N most expensive queries by bytes processed."""
        client = self._get_client()
        start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

        sql = f"""
        SELECT
            MD5(query)                                        AS query_hash,
            ANY_VALUE(query)                                  AS query_text,
            COUNT(*)                                          AS execution_count,
            AVG(total_bytes_processed)                        AS avg_bytes,
            SUM(total_bytes_processed)                        AS total_bytes,
            AVG(total_slot_ms)                                AS avg_slot_ms
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= '{start_date}'
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
        GROUP BY 1
        ORDER BY total_bytes DESC
        LIMIT {top_n}
        """
        rows = client.query(sql).result()
        summaries = []
        for row in rows:
            total_tb = row.total_bytes / 1e12
            cost = total_tb * BQ_PRICE_PER_TB
            rec = self._query_recommendation(row.query_text, row.avg_bytes)
            summaries.append(
                QueryCostSummary(
                    query_hash=row.query_hash.hex() if row.query_hash else "",
                    query_text=row.query_text[:500],
                    execution_count=row.execution_count,
                    avg_bytes_processed=int(row.avg_bytes),
                    total_bytes_processed=int(row.total_bytes),
                    avg_slot_ms=int(row.avg_slot_ms),
                    estimated_cost_usd=round(cost, 4),
                    recommendation=rec,
                )
            )
        return summaries

    def analyse_table_storage(self, dataset_id: str) -> list[TableCostSummary]:
        """Summarise storage costs per table in a dataset."""
        client = self._get_client()
        sql = f"""
        SELECT
            table_id,
            size_bytes / POW(1024, 3)         AS size_gb,
            active_logical_bytes / POW(1024, 3)   AS active_logical_gb,
            long_term_logical_bytes / POW(1024, 3) AS long_term_logical_gb,
            TIMESTAMP_MILLIS(last_modified_time)  AS last_modified
        FROM `{self.project_id}.{dataset_id}.__TABLES__`
        ORDER BY size_bytes DESC
        """
        rows = client.query(sql).result()
        summaries = []
        for row in rows:
            monthly_cost = (
                row.active_logical_gb * BQ_ACTIVE_STORAGE_PER_GB_MONTH
                + row.long_term_logical_gb * BQ_LONG_TERM_STORAGE_PER_GB_MONTH
            )
            rec = self._storage_recommendation(row.last_modified, row.size_gb)
            summaries.append(
                TableCostSummary(
                    table_id=row.table_id,
                    size_gb=round(row.size_gb, 3),
                    active_logical_gb=round(row.active_logical_gb, 3),
                    long_term_logical_gb=round(row.long_term_logical_gb, 3),
                    monthly_storage_cost_usd=round(monthly_cost, 4),
                    last_modified=row.last_modified.date() if row.last_modified else None,
                    recommendation=rec,
                )
            )
        return summaries

    def build_report(self, dataset_id: str = "lakehouse_gold") -> CostReport:
        query_summaries = self.analyse_expensive_queries()
        table_summaries = self.analyse_table_storage(dataset_id)

        total_query_cost = sum(q.estimated_cost_usd for q in query_summaries)
        total_storage_cost = sum(t.monthly_storage_cost_usd for t in table_summaries)
        total_cost = total_query_cost + total_storage_cost

        recs = [s.recommendation for s in query_summaries if s.recommendation]
        recs += [s.recommendation for s in table_summaries if s.recommendation]

        return CostReport(
            project_id=self.project_id,
            report_date=date.today(),
            query_summaries=query_summaries,
            table_summaries=table_summaries,
            total_estimated_monthly_cost_usd=round(total_cost, 2),
            potential_savings_usd=round(total_cost * 0.3, 2),
            top_recommendations=recs[:10],
        )

    @staticmethod
    def _query_recommendation(query_text: str, avg_bytes: float) -> str | None:
        gb = avg_bytes / 1e9
        if gb > 500:
            return f"Query scans {gb:.0f} GB on average — add partition filter or clustering."
        if "SELECT *" in query_text.upper():
            return "Avoid SELECT * — specify only needed columns to reduce bytes scanned."
        return None

    @staticmethod
    def _storage_recommendation(last_modified: Any, size_gb: float) -> str | None:
        if last_modified is None:
            return None
        days_old = (
            (date.today() - last_modified.date()).days if hasattr(last_modified, "date") else 0
        )
        if days_old > 365 and size_gb > 10:
            return f"Table not modified in {days_old}d ({size_gb:.1f} GB) — consider archiving."
        return None
