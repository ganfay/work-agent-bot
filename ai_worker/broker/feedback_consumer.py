"""
FEEDBACK CONSUMER & PREFERENCES ENGINE
Слухає чергу feedback_queue з Telegram, зберігає реджекти в PostgreSQL
та надає історію вподобань користувача для Агента 1 (Analyzer).
"""

import json
import logging
import threading
import time
import psycopg2
import pika

from config.config import settings

logger = logging.getLogger("ai_worker")


def get_recent_feedback_reasons(limit: int = 5) -> list[str]:
    """Вибирає останні унікальні причини відхилень із PostgreSQL для пам'яті ШІ."""
    conn = None
    try:
        conn = psycopg2.connect(settings.database_url())
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT reason
                FROM user_feedback
                GROUP BY reason
                ORDER BY MAX(created_at) DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception as err:
        logger.error(f"[FeedbackEngine] Failed to read feedback memory: {err}")
        return []
    finally:
        if conn is not None:
            conn.close()


def _persist_feedback(pg_url: str, job_title: str, reason: str, conn=None):
    owned_conn = conn is None
    db_conn = conn if conn is not None else psycopg2.connect(pg_url)
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO user_feedback (job_title, reason, status)
            VALUES (%s, %s, 'rejected');
            """,
            (job_title, reason),
        )
        db_conn.commit()
    except Exception:
        try:
            db_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()
        if owned_conn:
            db_conn.close()


def start_feedback_listener_thread(stop_event: threading.Event | None = None):
    """Запускає фоновий потік, який слухає feedback_queue з RabbitMQ із автоматичним перепідключенням."""
    def run():
        backoff = 1.0
        while stop_event is None or not stop_event.is_set():
            connection = None
            try:
                params = pika.URLParameters(settings.rabbitmq_url())
                params.heartbeat = 600
                params.blocked_connection_timeout = 300
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.queue_declare(queue="feedback_queue", durable=True)
                channel.basic_qos(prefetch_count=1)

                backoff = 1.0
                logger.info("[FeedbackEngine] Background listener running for 'feedback_queue'...")

                def callback(ch, method, properties, body):
                    try:
                        data = json.loads(body.decode("utf-8"))
                        if not isinstance(data, dict):
                            raise ValueError("feedback payload must be a JSON object")
                        job_title = str(data.get("job_title", "Unknown"))
                        reason = str(data.get("reason", "No reason"))
                    except Exception as e:
                        logger.error(f"[FeedbackEngine] Invalid feedback payload: {e}")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        return

                    try:
                        logger.info(f"[FeedbackEngine] Captured human rejection for '{job_title}': '{reason}'")
                        _persist_feedback(settings.database_url(), job_title, reason)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        logger.info("[FeedbackEngine] Stored feedback in PostgreSQL successfully.")
                    except Exception as e:
                        logger.error(f"[FeedbackEngine] Error storing feedback in PostgreSQL: {e}")
                        requeue = not method.redelivered
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)

                channel.basic_consume(queue="feedback_queue", on_message_callback=callback)
                channel.start_consuming()

            except Exception as e:
                if stop_event is not None and stop_event.is_set():
                    break
                logger.error(
                    f"[FeedbackEngine] Feedback listener disconnected: {e}. Reconnecting in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            finally:
                if connection is not None and not connection.is_closed:
                    try:
                        connection.close()
                    except Exception:
                        pass

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def drain_feedback_queue(amqp_url: str | None = None, db_url: str | None = None) -> int:
    """Synchronously drains all pending messages from feedback_queue into PostgreSQL."""
    rmq_url = amqp_url or settings.rabbitmq_url()
    pg_url = db_url or settings.database_url()
    processed = 0
    connection = None
    pg_conn = None
    try:
        params = pika.URLParameters(rmq_url)
        params.heartbeat = 600
        params.blocked_connection_timeout = 300
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="feedback_queue", durable=True)

        while True:
            method_frame, _, body = channel.basic_get(queue="feedback_queue", auto_ack=False)
            if method_frame is None:
                break
            try:
                data = json.loads(body.decode("utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("feedback payload must be a JSON object")
                job_title = str(data.get("job_title", "Unknown"))
                reason = str(data.get("reason", "No reason"))

                if pg_conn is None or getattr(pg_conn, "closed", 0):
                    pg_conn = psycopg2.connect(pg_url)

                _persist_feedback(pg_url, job_title, reason, conn=pg_conn)

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                processed += 1
                logger.info(f"[FeedbackEngine] Drained feedback for '{job_title}': '{reason}'")
            except Exception as e:
                logger.error(f"[FeedbackEngine] Error draining feedback message: {e}")
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
    except Exception as e:
        logger.error(f"[FeedbackEngine] Failed to drain feedback_queue: {e}")
    finally:
        if pg_conn is not None:
            try:
                pg_conn.close()
            except Exception:
                pass
        if connection is not None and not connection.is_closed:
            connection.close()
    return processed
