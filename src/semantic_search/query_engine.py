"""NL Query Engine — translates natural language questions to SQL via LangChain RAG."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

SQL_GENERATION_PROMPT_TEMPLATE = """You are an expert SQL analyst for a data lakehouse on GCP.

Relevant table schemas (from semantic similarity search):
{context}

Generate a valid Trino SQL query to answer the following question.
Return ONLY the SQL query — no explanation, no markdown fences.

Question: {question}

SQL:"""

EXPLANATION_PROMPT_TEMPLATE = """You are a data analyst explaining query results.

Original question: {question}

Query executed:
{sql}

Result (first 20 rows):
{rows}

Provide a concise, plain-English answer to the question based on these results. Focus on insights, not mechanics."""


@dataclass
class QueryResult:
    question: str
    sql: str
    rows: list[dict[str, Any]]
    answer: str
    schema_context: str


class NLQueryEngine:
    """Translates natural language to SQL, executes via Trino, explains results."""

    def __init__(
        self,
        trino_conn: Any,
        metadata_indexer: Any,
        *,
        model_name: str = "gemini-1.5-pro",
        top_k_schemas: int = 5,
    ) -> None:
        self.trino = trino_conn
        self.indexer = metadata_indexer
        self.model_name = model_name
        self.top_k = top_k_schemas
        self._llm: Any = None

    def _get_llm(self) -> Any:
        if self._llm is None:
            from langchain_google_vertexai import VertexAI
            self._llm = VertexAI(model_name=self.model_name, temperature=0.0)
        return self._llm

    async def query(self, question: str) -> QueryResult:
        # Retrieve relevant schemas via semantic similarity
        schema_docs = self.indexer.search(question, k=self.top_k)
        context = "\n\n".join(
            d.page_content if hasattr(d, "page_content") else str(d)
            for d in schema_docs
        )

        # Generate SQL
        sql = await self._generate_sql(question, context)
        sql = sql.strip().rstrip(";")

        # Execute against Trino
        rows = self._execute_sql(sql)

        # Explain in plain English
        answer = await self._explain(question, sql, rows)

        return QueryResult(
            question=question,
            sql=sql,
            rows=rows,
            answer=answer,
            schema_context=context,
        )

    async def _generate_sql(self, question: str, context: str) -> str:
        from langchain.chains import LLMChain
        from langchain.prompts import PromptTemplate

        prompt = PromptTemplate(
            input_variables=["context", "question"],
            template=SQL_GENERATION_PROMPT_TEMPLATE,
        )
        chain = LLMChain(llm=self._get_llm(), prompt=prompt)
        return await chain.arun(context=context, question=question)

    def _execute_sql(self, sql: str) -> list[dict[str, Any]]:
        cursor = self.trino.cursor()
        cursor.execute(sql)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchmany(100)]

    async def _explain(
        self, question: str, sql: str, rows: list[dict[str, Any]]
    ) -> str:
        llm = self._get_llm()
        prompt = EXPLANATION_PROMPT_TEMPLATE.format(
            question=question,
            sql=sql,
            rows="\n".join(str(r) for r in rows[:20]),
        )
        return await llm.apredict(prompt)
