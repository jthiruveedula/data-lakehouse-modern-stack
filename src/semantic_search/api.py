"""FastAPI app for the semantic search / natural language query endpoint."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_engine: Any = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine
    # Initialise connections only in real deployments
    if os.getenv("LAKEHOUSE_ENV") == "production":
        import trino
        from src.semantic_search.metadata_indexer import MetadataIndexer
        from src.semantic_search.query_engine import NLQueryEngine

        conn = trino.dbapi.connect(
            host=os.environ["TRINO_HOST"],
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "lakehouse"),
            catalog="delta",
        )
        indexer = MetadataIndexer(persist_directory=os.getenv("INDEX_DIR", "/tmp/catalog_index"))
        _engine = NLQueryEngine(conn, indexer)
    yield


app = FastAPI(
    title="Data Lakehouse Semantic Search",
    description="Natural language query engine powered by LangChain + Vertex AI + Trino",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    question: str
    max_rows: int = 100


class QueryResponse(BaseModel):
    question: str
    sql: str
    rows: list[dict]
    answer: str
    row_count: int


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "lakehouse-semantic-search"}


@app.post("/query", response_model=QueryResponse)
async def query(req: QueryRequest) -> QueryResponse:
    if _engine is None:
        raise HTTPException(503, "Query engine not initialised (set LAKEHOUSE_ENV=production)")
    try:
        result = await _engine.query(req.question)
        return QueryResponse(
            question=result.question,
            sql=result.sql,
            rows=result.rows[: req.max_rows],
            answer=result.answer,
            row_count=len(result.rows),
        )
    except Exception as exc:
        logger.exception("Query failed")
        raise HTTPException(500, str(exc)) from exc


@app.get("/tables")
async def list_tables() -> dict[str, Any]:
    """Return indexed table names for discoverability."""
    if _engine is None:
        return {"tables": [], "note": "Engine not initialised"}
    docs = _engine.indexer.search("", k=50)
    names = [d.metadata.get("full_name") for d in docs if hasattr(d, "metadata")]
    return {"tables": names, "count": len(names)}
