"""
FEEDBACK CONSUMER & PREFERENCES ENGINE
Слухає чергу feedback_queue з Telegram, зберігає реджекти в PostgreSQL
та надає історію вподобань користувача для Агента 1 (Analyzer).
"""

import json
import logging
import threading
import psycopg2
import pika

from config.config import settings

logger = logging.getLogger("ai_worker")


def get_recent_feedback_reasons(limit: int = 5) -> list[str]:
    """Вибирає останні унікальні причини відхилень із PostgreSQL для пам'яті ШІ."""
    try:
        conn = psycopg2.connect(settings.database_url())
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT reason 
            FROM user_feedback 
            ORDER BY reason 
            LIMIT %s;
            """,
            (limit,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception as err:
        logger.error(f"[FeedbackEngine] Failed to read feedback memory: {err}")
        return []


def start_feedback_listener_thread():
    """Запускає фоновий потік, який слухає feedback_queue з RabbitMQ."""
    def run():
        try:
            params = pika.URLParameters(settings.rabbitmq_url())
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue="feedback_queue", durable=True)

            logger.info("[FeedbackEngine] Background listener running for 'feedback_queue'...")

            def callback(ch, method, properties, body):
                try:
                    data = json.loads(body.decode("utf-8"))
                    job_title = data.get("job_title", "Unknown")
                    reason = data.get("reason", "No reason")

                    logger.info(f"[FeedbackEngine] Captured human rejection for '{job_title}': '{reason}'")

                    # Записуємо в базу PostgreSQL
                    conn = psycopg2.connect(settings.database_url())
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO user_feedback (job_title, reason, status)
                        VALUES (%s, %s, 'rejected');
                        """,
                        (job_title, reason)
                    )
                    conn.commit()
                    cur.close()
                    conn.close()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info(f"[FeedbackEngine] Stored feedback in PostgreSQL successfully.")

                except Exception as e:
                    logger.error(f"[FeedbackEngine] Error processing feedback message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue="feedback_queue", on_message_callback=callback)
            channel.start_consuming()

        except Exception as e:
            logger.error(f"[FeedbackEngine] Feedback listener thread crashed: {e}")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread
