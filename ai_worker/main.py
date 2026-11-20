import re
import signal
import sys
import threading
import time
import psycopg2

from broker.consumer import RabbitMQConsumer
from broker.feedback_consumer import (
    start_feedback_listener_thread,
    drain_feedback_queue,
    get_recent_feedback_reasons,
)
from config.config import settings
from core.llm import create_genai_client
from core.logger import setup_logger
from core.sanitize import (
    MAX_DESCRIPTION_LEN,
    MAX_TITLE_LEN,
    redact_url,
    sanitize_text,
)
from agents.analyzer import VacancyAnalyzer
from agents.validator import EvidenceValidator
from agents.drafter import CoverLetterDrafter
from agents.critic import CoverLetterCritic
from rag.embedder import embedder
from rag.indexer import index_knowledge_base

# 1. Initialize logger
logger = setup_logger(settings.log_path)

# 2. Self-healing check: ensure pgvector has codebase embeddings
def ensure_embeddings_exist(max_attempts: int = 5, base_delay: float = 2.0):
    count = None
    last_err = None
    for attempt in range(1, max_attempts + 1):
        conn = None
        try:
            conn = psycopg2.connect(**settings.pg_connect_kwargs(statement_timeout_ms=30000))
            with conn:
                with conn.cursor() as cur:
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
                    cur.execute("SELECT COUNT(*) FROM code_embeddings;")
                    count = cur.fetchone()[0]
            last_err = None
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as err:
            last_err = err
            if attempt >= max_attempts:
                logger.critical(
                    f"[RAG] PostgreSQL unreachable after {max_attempts} startup attempts: {redact_url(err)}"
                )
                raise
            delay = base_delay * attempt
            logger.warning(
                f"[RAG] PostgreSQL not ready (attempt {attempt}/{max_attempts}): {redact_url(err)}. "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)
        except Exception as err:
            logger.critical(f"[RAG] Fatal error checking vector database: {redact_url(err)}")
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    if last_err is not None:
        raise last_err

    if count == 0:
        logger.warning("[RAG] Vector database is empty! Triggering automatic indexing...")
        try:
            index_knowledge_base()
        except Exception as err:
            logger.critical(f"[RAG] Automatic knowledge base indexing failed: {redact_url(err)}")
            raise
    else:
        logger.info(f"[RAG] Vector database initialized with {count} code chunks.")

genai_client = None
analyzer = None
validator = None
drafter = None
critic = None


def init_agents():
    global genai_client, analyzer, validator, drafter, critic
    if not settings.gemini_api_key:
        logger.critical("GEMINI_API_KEY is not set in .env! Exiting.")
        sys.exit(1)

    genai_client = create_genai_client(settings.gemini_api_key)
    embedder.set_client(genai_client)
    analyzer = VacancyAnalyzer(api_key=settings.gemini_api_key, client=genai_client)
    validator = EvidenceValidator(api_key=settings.gemini_api_key, client=genai_client)
    drafter = CoverLetterDrafter(api_key=settings.gemini_api_key, client=genai_client)
    critic = CoverLetterCritic(api_key=settings.gemini_api_key, client=genai_client)


def close_agents():
    global genai_client, analyzer, validator, drafter, critic
    for agent in (analyzer, validator, drafter, critic):
        if agent is not None and hasattr(agent, "close"):
            try:
                agent.close()
            except Exception:
                pass
    try:
        embedder.set_client(None)
        embedder.close()
    except Exception:
        pass
    if genai_client is not None:
        try:
            genai_client.close()
        except Exception:
            pass
        finally:
            genai_client = None
    analyzer = None
    validator = None
    drafter = None
    critic = None


# 5. Vacancy processing handler
def process_vacancy(job: dict, publisher_func):
    title = sanitize_text(job.get("Title") or "No Title", MAX_TITLE_LEN, single_line=True) or "No Title"
    link = re.sub(r"[\x00-\x1f\x7f]+", "", str(job.get("Link") or "")).strip()
    description = sanitize_text(job.get("Description") or "", MAX_DESCRIPTION_LEN)

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
    summary = analysis.summary
    if getattr(critique, "fallback_used", False):
        logger.warning(f"[Critic] Fallback used for '{title}'; flagging summary for human review.")
        summary = f"[⚠️ Critic audit skipped — unreviewed draft] {summary}"
    elif getattr(critique, "hallucinations_detected", None) or not getattr(critique, "is_approved", True):
        hallucinations = getattr(critique, "hallucinations_detected", None) or []
        logger.warning(
            f"[Critic] Draft flagged for '{title}' "
            f"(approved={critique.is_approved}, hallucinations={len(hallucinations)})."
        )
        summary = f"[⚠️ Critic flagged draft (approved={critique.is_approved})] {summary}"

    review_payload = {
        "title": title,
        "link": link,
        "match_score": analysis.match_score,
        "summary": summary,
        "pros": analysis.pros,
        "fluff_percentage": critique.fluff_percentage,
        "final_letter": critique.final_letter,
    }

    publisher_func(review_payload)
    logger.info(f"[+] Vacancy '{title}' forwarded to review_queue for Telegram Bot!")


def main():
    logger.info(
        f"Starting AI Worker Service (run_once={settings.run_once}, "
        f"batch_idle_timeout={settings.batch_idle_timeout}s)..."
    )

    init_agents()
    ensure_embeddings_exist()

    consumer = RabbitMQConsumer(
        queue_name=settings.queue_name,
        connection_params=settings.rmq_connection_parameters(),
    )
    stop_event = threading.Event()
    feedback_thread = None

    def _handle_shutdown(signum, frame):
        logger.info(f"Received signal {signum}; requesting graceful shutdown...")
        stop_event.set()
        try:
            consumer.request_stop()
        except Exception:
            pass

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handle_shutdown)
        signal.signal(signal.SIGINT, _handle_shutdown)

    try:
        if settings.run_once:
            drain_feedback_queue()
            consumer.connect()
            consumer.consume_batch(
                message_handler=process_vacancy,
                idle_timeout=settings.batch_idle_timeout,
                stop_event=stop_event,
            )
            if not stop_event.is_set():
                drain_feedback_queue()
            logger.info("RUN_ONCE=true: AI Worker batch run completed, exiting cleanly.")
            return

        feedback_thread = start_feedback_listener_thread(stop_event=stop_event)
        consumer.connect()
        consumer.start_consuming(message_handler=process_vacancy, stop_event=stop_event)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Closing worker...")
        stop_event.set()
        sys.exit(0)

    except Exception as err:
        logger.critical(f"Fatal worker error: {redact_url(err)}", exc_info=True)
        stop_event.set()
        sys.exit(1)

    finally:
        stop_event.set()
        consumer.close()
        if feedback_thread is not None:
            feedback_thread.join(timeout=5.0)
        close_agents()


if __name__ == "__main__":
    main()
