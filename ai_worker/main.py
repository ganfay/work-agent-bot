import sys
import psycopg2

from broker.consumer import RabbitMQConsumer
from broker.feedback_consumer import start_feedback_listener_thread, get_recent_feedback_reasons
from config.config import settings
from core.logger import setup_logger
from agents.analyzer import VacancyAnalyzer
from agents.validator import EvidenceValidator
from agents.drafter import CoverLetterDrafter
from agents.critic import CoverLetterCritic
from rag.indexer import index_knowledge_base

# 1. Initialize logger
logger = setup_logger(settings.log_path)

# 2. Self-healing check: ensure pgvector has codebase embeddings
def ensure_embeddings_exist():
    try:
        conn = psycopg2.connect(settings.database_url())
        cur = conn.cursor()
        cur.execute("""
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
            CREATE TABLE IF NOT EXISTS user_feedback (
                id SERIAL PRIMARY KEY,
                job_title TEXT NOT NULL,
                reason TEXT NOT NULL,
                status VARCHAR(20) DEFAULT 'rejected',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM code_embeddings;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        if count == 0:
            logger.warning("[RAG] Vector database is empty! Triggering automatic indexing...")
            index_knowledge_base()
        else:
            logger.info(f"[RAG] Vector database initialized with {count} code chunks.")
    except Exception as e:
        logger.error(f"[RAG] Error checking vector database: {e}")

ensure_embeddings_exist()

# 3. Start background feedback listener (RabbitMQ -> PostgreSQL user_feedback)
start_feedback_listener_thread()

# 4. Initialize our 4 agents
if not settings.gemini_api_key:
    logger.critical("GEMINI_API_KEY is not set in .env! Exiting.")
    sys.exit(1)

analyzer = VacancyAnalyzer(api_key=settings.gemini_api_key)
validator = EvidenceValidator(api_key=settings.gemini_api_key)
drafter = CoverLetterDrafter(api_key=settings.gemini_api_key)
critic = CoverLetterCritic(api_key=settings.gemini_api_key)


# 5. Vacancy processing handler
def process_vacancy(job: dict, publisher_func):
    title = job.get("Title", "No Title")
    link = job.get("Link", "")
    description = job.get("Description", "")

    logger.info("==================================================")
    logger.info(f"Incoming job from queue: '{title}'")
    logger.info(f"Link: {link}")

    # Отримуємо свіжу пам'ять про вподобання Максима (реджекти з Telegram)
    learned_feedback = get_recent_feedback_reasons()
    if learned_feedback:
        logger.info(f"[Memory] Loaded {len(learned_feedback)} active user rejection preferences: {learned_feedback}")

    # ----------------------------------------------------
    # AGENT 1: ANALYZER (Fast filtering with adaptive memory)
    # ----------------------------------------------------
    analysis = analyzer.analyze(title=title, description=description, recent_feedback=learned_feedback)
    logger.info(f"--> [1. ANALYZER] Score: {analysis.match_score}/100 | Decision: {analysis.decision}")

    if analysis.decision == "SKIP":
        logger.info(f"[-] Vacancy '{title}' skipped (score too low). Pipeline finished.")
        return

    # ----------------------------------------------------
    # AGENT 2: EVIDENCE VALIDATOR (Vector RAG via pgvector)
    # ----------------------------------------------------
    validation = validator.validate(extracted_stack=analysis.extracted_stack)
    logger.info(f"--> [2. VALIDATOR:pgvector] Verified: {len(validation.verified_skills)} | Gaps: {len(validation.unverified_skills)}")
    for s in validation.verified_skills:
        logger.info(f"    * {s.skill} ({s.project}): {s.evidence}")

    # ----------------------------------------------------
    # AGENT 3: COVER LETTER DRAFTER (Initial draft generation)
    # ----------------------------------------------------
    draft = drafter.draft(title=title, description=description, validation=validation)
    logger.info(f"--> [3. DRAFTER] Draft generated ({draft.word_count} words).")

    # ----------------------------------------------------
    # AGENT 4: CRITIC & QUALITY ASSURANCE (De-AI-ify and Polish)
    # ----------------------------------------------------
    critique = critic.review(job_title=title, draft=draft, validation=validation)
    logger.info(f"--> [4. CRITIC] Overall Score: {critique.critique_score}/100 | Fluff: {critique.fluff_percentage}%")

    logger.info("==================================================")
    logger.info("FINAL PRODUCTION-READY PROPOSAL:")
    logger.info("--------------------------------------------------")
    logger.info(f"\n{critique.final_letter}\n")
    logger.info("--------------------------------------------------")

    # ----------------------------------------------------
    # PUBLISH TO REVIEW QUEUE (For Go Telegram Bot HITL)
    # ----------------------------------------------------
    review_payload = {
        "title": title,
        "link": link,
        "match_score": analysis.match_score,
        "summary": analysis.summary,
        "pros": analysis.pros,
        "fluff_percentage": critique.fluff_percentage,
        "final_letter": critique.final_letter,
    }

    publisher_func(review_payload)
    logger.info(f"[+] Vacancy '{title}' forwarded to review_queue for Telegram Bot!")


def main():
    logger.info("Starting AI Worker Service with full 4-Agent Pipeline, pgvector & Feedback Engine...")

    consumer = RabbitMQConsumer(
        amqp_url=settings.rabbitmq_url(),
        queue_name=settings.queue_name
    )

    try:
        consumer.connect()
        consumer.start_consuming(message_handler=process_vacancy)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (Ctrl+C). Closing worker...")
        consumer.close()
        sys.exit(0)

    except Exception as err:
        logger.critical(f"Fatal worker error: {err}", exc_info=True)
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
