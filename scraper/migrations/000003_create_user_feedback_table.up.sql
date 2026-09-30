CREATE TABLE IF NOT EXISTS user_feedback (
    id SERIAL PRIMARY KEY,
    job_title TEXT NOT NULL,
    reason TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'rejected',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
