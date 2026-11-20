from typing import List, Dict, Any
import psycopg2
from pgvector import Vector
from pgvector.psycopg2 import register_vector

from config.config import settings
from rag.embedder import embedder

_SIMILARITY_SQL = """
    SELECT
        project_name,
        category,
        content,
        1 - (embedding <=> %s) AS similarity
    FROM code_embeddings
    ORDER BY embedding <=> %s ASC
    LIMIT %s;
"""


class VectorRetriever:
    def _query_vector_with_cursor(
        self, cursor, query_vector: List[float], limit: int, threshold: float
    ) -> List[Dict[str, Any]]:
        vec = Vector(list(query_vector))
        cursor.execute(_SIMILARITY_SQL, (vec, vec, limit))
        rows = cursor.fetchall()
        matches: List[Dict[str, Any]] = []
        for row in rows:
            sim = float(row[3])
            if sim < threshold:
                continue
            matches.append(
                {
                    "project": row[0],
                    "category": row[1],
                    "content": row[2],
                    "similarity": round(sim, 3),
                }
            )
        return matches

    def search_evidence(self, query: str, limit: int = 2, threshold: float = 0.50) -> List[Dict[str, Any]]:
        if limit <= 0 or not isinstance(query, str) or not query.strip():
            return []
        query_vector = embedder.embed_text(query.strip())
        conn = psycopg2.connect(**settings.pg_connect_kwargs())
        try:
            register_vector(conn)
            with conn.cursor() as cursor:
                return self._query_vector_with_cursor(cursor, query_vector, limit, threshold)
        finally:
            conn.close()

    def search_evidence_batch(
        self, queries: List[str], limit: int = 1, threshold: float = 0.48
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Searches pgvector for multiple technologies while deduplicating and reusing a single PostgreSQL connection."""
        if not queries:
            return {}
        if limit <= 0:
            return {q: [] for q in queries}

        unique_queries: List[str] = []
        norm_to_canonical: Dict[str, str] = {}
        query_norms: List[tuple[str, str]] = []

        for q in queries:
            if not isinstance(q, str) or not q.strip():
                query_norms.append((q, ""))
                continue
            cleaned = q.strip()
            norm = cleaned.casefold()
            if norm not in norm_to_canonical:
                norm_to_canonical[norm] = cleaned
                unique_queries.append(cleaned)
            query_norms.append((q, norm))

        if not unique_queries:
            return {q: [] for q, _ in query_norms}

        vectors = embedder.embed_texts(unique_queries)
        if len(vectors) != len(unique_queries):
            raise RuntimeError(
                f"Embedder returned {len(vectors)} vectors for {len(unique_queries)} unique queries"
            )

        conn = psycopg2.connect(**settings.pg_connect_kwargs())
        try:
            register_vector(conn)
            matches_by_norm: Dict[str, List[Dict[str, Any]]] = {}
            with conn.cursor() as cursor:
                for canonical, vec in zip(unique_queries, vectors):
                    matches_by_norm[canonical.casefold()] = self._query_vector_with_cursor(
                        cursor, vec, limit, threshold
                    )
            return {
                orig_q: ([dict(item) for item in matches_by_norm.get(norm, [])] if norm else [])
                for orig_q, norm in query_norms
            }
        finally:
            conn.close()


retriever = VectorRetriever()
