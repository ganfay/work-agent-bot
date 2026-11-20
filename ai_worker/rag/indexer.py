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
from pgvector import Vector
from pgvector.psycopg2 import register_vector

from config.config import settings
from core.logger import setup_logger
from rag.embedder import embedder

logger = setup_logger(settings.log_path)

MAX_PROJECT_NAME_LEN = 100
MAX_CATEGORY_LEN = 50


def _validate_chunks(chunks) -> list[tuple[str, str, str]]:
    if not isinstance(chunks, list):
        raise ValueError("knowledge_chunks must be a list")

    validated: list[tuple[str, str, str]] = []
    for idx, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            raise ValueError(f"knowledge_chunks[{idx}] must be an object")

        for field in ("project", "category", "content"):
            val = chunk.get(field)
            if not isinstance(val, str) or not val.strip():
                raise ValueError(f"knowledge_chunks[{idx}].{field} must be a non-empty string")

        project = chunk["project"].strip()
        category = chunk["category"].strip()
        content = chunk["content"].strip()

        if len(project) > MAX_PROJECT_NAME_LEN:
            raise ValueError(
                f"knowledge_chunks[{idx}].project exceeds max length {MAX_PROJECT_NAME_LEN}"
            )
        if len(category) > MAX_CATEGORY_LEN:
            raise ValueError(
                f"knowledge_chunks[{idx}].category exceeds max length {MAX_CATEGORY_LEN}"
            )
        validated.append((project, category, content))
    return validated


def index_knowledge_base():
    chunks = settings.profile_data.get("knowledge_chunks", [])
    if chunks == [] or chunks is None:
        logger.error("No knowledge chunks found in candidate_profile.json to index!")
        return

    validated_chunks = _validate_chunks(chunks)
    if not validated_chunks:
        logger.error("No knowledge chunks found in candidate_profile.json to index!")
        return

    logger.info(f"Generating embeddings for {len(validated_chunks)} knowledge chunks before updating pgvector...")
    prepared_rows = []
    for i, (project, category, content) in enumerate(validated_chunks, start=1):
        vector = embedder.embed_text(content)
        if not vector:
            raise ValueError(f"Empty embedding vector returned for chunk {i} ({project} - {category})")
        prepared_rows.append((project, category, content, list(vector)))
        logger.info(f"[{i}/{len(validated_chunks)}] Embedded: {project} - {category}")

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
                        (project, category, content, Vector(vector)),
                    )
        logger.info(f"Successfully indexed {len(prepared_rows)} knowledge chunks into pgvector!")
    finally:
        conn.close()


if __name__ == "__main__":
    index_knowledge_base()
