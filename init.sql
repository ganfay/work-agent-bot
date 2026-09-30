-- PostgreSQL Initialization Schema for Freelance Agent
CREATE EXTENSION IF NOT EXISTS vector;

-- 1. Scraper Jobs Deduplication Table
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    guid VARCHAR UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status VARCHAR DEFAULT 'new',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Codebase Vector Embeddings Table
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

-- 3. Human-in-the-Loop Feedback & Preference Memory Table
CREATE TABLE IF NOT EXISTS user_feedback (
    id SERIAL PRIMARY KEY,
    job_title TEXT NOT NULL,
    reason TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'rejected',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
