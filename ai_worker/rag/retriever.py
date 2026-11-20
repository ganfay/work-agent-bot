from typing import List, Dict, Any
import psycopg2
from pgvector.psycopg2 import register_vector

from config.config import settings
from rag.embedder import embedder

_SIMILARITY_SQL = """
    SELECT
        project_name,
        category,
        content,
        1 - (embedding <=> %s::vector) AS similarity
    FROM code_embeddings
    WHERE 1 - (embedding <=> %s::vector) >= %s
    ORDER BY similarity DESC
    LIMIT %s;
"""


class VectorRetriever:
    def _query_with_cursor(self, cursor, query: str, limit: int, threshold: float) -> List[Dict[str, Any]]:
        query_vector = embedder.embed_text(query)
        cursor.execute(_SIMILARITY_SQL, (query_vector, query_vector, threshold, limit))
        rows = cursor.fetchall()
        return [
            {
                "project": row[0],
                "category": row[1],
                "content": row[2],
                "similarity": round(float(row[3]), 3),
            }
            for row in rows
        ]

    def search_evidence(self, query: str, limit: int = 2, threshold: float = 0.50) -> List[Dict[str, Any]]:
        conn = psycopg2.connect(**settings.pg_connect_kwargs())
        try:
            register_vector(conn)
            with conn.cursor() as cursor:
                return self._query_with_cursor(cursor, query, limit, threshold)
        finally:
            conn.close()

    def search_evidence_batch(
        self, queries: List[str], limit: int = 1, threshold: float = 0.48
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Searches pgvector for multiple technologies while reusing a single PostgreSQL connection."""
        if not queries:
            return {}
        conn = psycopg2.connect(**settings.pg_connect_kwargs())
        try:
            register_vector(conn)
            results: Dict[str, List[Dict[str, Any]]] = {}
            with conn.cursor() as cursor:
                for q in queries:
                    results[q] = self._query_with_cursor(cursor, q, limit, threshold)
            return results
        finally:
            conn.close()


retriever = VectorRetriever()
