"""
CODEBASE INDEXER SCRIPT
Завантажує фрагменти знань про реальні проєкти з candidate_profile.json,
генерує для них векторні ембединги і записує в PostgreSQL у таблицю code_embeddings.
"""

import sys
from pathlib import Path

AI_WORKER_ROOT = str(Path(__file__).resolve().parent.parent)
if AI_WORKER_ROOT not in sys.path:
    sys.path.insert(0, AI_WORKER_ROOT)

import psycopg2
from pgvector.psycopg2 import register_vector

from config.config import settings
from core.logger import setup_logger
from rag.embedder import embedder

logger = setup_logger(settings.log_path)


def index_knowledge_base():
    chunks = settings.profile_data.get("knowledge_chunks", [])
    if not chunks:
        logger.error("No knowledge chunks found in candidate_profile.json to index!")
        return

    logger.info(f"Generating embeddings for {len(chunks)} knowledge chunks before updating pgvector...")
    prepared_rows = []
    for i, chunk in enumerate(chunks, start=1):
        vector = embedder.embed_text(chunk["content"])
        prepared_rows.append((chunk["project"], chunk["category"], chunk["content"], vector))
        logger.info(f"[{i}/{len(chunks)}] Embedded: {chunk['project']} - {chunk['category']}")

    logger.info("Connecting to PostgreSQL to atomically replace codebase embeddings...")
    conn = psycopg2.connect(**settings.pg_connect_kwargs(statement_timeout_ms=30000))
    try:
        register_vector(conn)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE code_embeddings;")
                for project, category, content, vector in prepared_rows:
                    cursor.execute(
                        """
                        INSERT INTO code_embeddings (project_name, category, content, embedding)
                        VALUES (%s, %s, %s, %s);
                        """,
                        (project, category, content, vector),
                    )
        logger.info(f"Successfully indexed {len(prepared_rows)} knowledge chunks into pgvector!")
    finally:
        conn.close()


if __name__ == "__main__":
    index_knowledge_base()
