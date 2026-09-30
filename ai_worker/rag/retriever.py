from typing import List, Dict, Any
import psycopg2
from pgvector.psycopg2 import register_vector

from config.config import settings
from rag.embedder import embedder


class VectorRetriever:
    def __init__(self):
        self.db_url = settings.database_url()

    def search_evidence(self, query: str, limit: int = 2, threshold: float = 0.50) -> List[Dict[str, Any]]:
        query_vector = embedder.embed_text(query)

        conn = psycopg2.connect(self.db_url)
        register_vector(conn)
        cursor = conn.cursor()

        sql = """
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

        cursor.execute(sql, (query_vector, query_vector, threshold, limit))
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                "project": row[0],
                "category": row[1],
                "content": row[2],
                "similarity": round(float(row[3]), 3)
            })

        cursor.close()
        conn.close()
        return results


retriever = VectorRetriever()
