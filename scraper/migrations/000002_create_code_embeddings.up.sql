CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS code_embeddings (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    embedding vector(768) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS code_embeddings_hnsw_idx
ON code_embeddings USING hnsw (embedding vector_cosine_ops);
