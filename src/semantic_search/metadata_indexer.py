"""Metadata indexer — embeds table schemas into a Chroma vector store for RAG."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    catalog: str
    schema: str
    table_name: str
    columns: list[dict[str, str]]
    description: str = ""
    sample_values: dict[str, list[Any]] | None = None
    row_count: int | None = None
    tags: list[str] | None = None

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_name}"

    def to_document(self) -> str:
        """Render metadata as a natural-language document for embedding."""
        col_lines = "\n".join(
            f"  - {c['name']} ({c['type']}): {c.get('description', '')}"
            for c in self.columns
        )
        samples = ""
        if self.sample_values:
            sample_lines = "\n".join(
                f"  {col}: {vals[:3]}" for col, vals in self.sample_values.items()
            )
            samples = f"\nSample values:\n{sample_lines}"

        return (
            f"Table: {self.full_name}\n"
            f"Description: {self.description}\n"
            f"Row count: {self.row_count or 'unknown'}\n"
            f"Columns:\n{col_lines}"
            f"{samples}"
        )


class MetadataIndexer:
    """Crawls Trino/BigQuery catalogs and indexes table metadata into Chroma."""

    def __init__(self, persist_directory: str = "/tmp/catalog_index") -> None:
        self.persist_directory = persist_directory
        self._vector_store: Any = None  # Chroma instance, lazy-loaded

    def _get_vector_store(self) -> Any:
        if self._vector_store is None:
            from chromadb import PersistentClient
            from langchain_community.vectorstores import Chroma
            from langchain_google_vertexai import VertexAIEmbeddings

            embeddings = VertexAIEmbeddings(model_name="textembedding-gecko@003")
            self._vector_store = Chroma(
                collection_name="lakehouse_metadata",
                embedding_function=embeddings,
                persist_directory=self.persist_directory,
            )
        return self._vector_store

    def index_table(self, metadata: TableMetadata) -> None:
        vs = self._get_vector_store()
        doc = metadata.to_document()
        vs.add_texts(
            texts=[doc],
            metadatas=[{"full_name": metadata.full_name, "catalog": metadata.catalog}],
            ids=[metadata.full_name],
        )
        logger.info("Indexed table: %s", metadata.full_name)

    def index_all(self, metadatas: list[TableMetadata]) -> None:
        vs = self._get_vector_store()
        texts = [m.to_document() for m in metadatas]
        metas = [{"full_name": m.full_name, "catalog": m.catalog} for m in metadatas]
        ids = [m.full_name for m in metadatas]
        vs.add_texts(texts=texts, metadatas=metas, ids=ids)
        logger.info("Indexed %d tables", len(metadatas))

    def search(self, query: str, k: int = 5) -> list[TableMetadata]:
        vs = self._get_vector_store()
        results = vs.similarity_search(query, k=k)
        return results

    def crawl_trino(self, trino_conn: Any, catalogs: list[str] | None = None) -> list[TableMetadata]:
        """Auto-discover all tables from Trino information_schema."""
        cursor = trino_conn.cursor()
        tables: list[TableMetadata] = []

        catalog_filter = (
            f"AND table_catalog IN ({', '.join(repr(c) for c in catalogs)})"
            if catalogs else ""
        )
        cursor.execute(
            f"SELECT table_catalog, table_schema, table_name "
            f"FROM information_schema.tables "
            f"WHERE table_schema NOT IN ('information_schema', 'system') {catalog_filter}"
        )
        for catalog, schema, tbl in cursor.fetchall():
            cols = self._get_columns(cursor, catalog, schema, tbl)
            tables.append(TableMetadata(
                catalog=catalog, schema=schema, table_name=tbl, columns=cols
            ))

        return tables

    def _get_columns(
        self, cursor: Any, catalog: str, schema: str, table: str
    ) -> list[dict[str, str]]:
        cursor.execute(
            f"SELECT column_name, data_type "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )
        return [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
