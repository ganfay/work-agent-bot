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

from broker.consumer import (
    build_pika_parameters,
    is_channel_open,
    is_connection_open,
    publish_to_dlq,
    safe_ack,
    safe_nack,
)
from config.config import settings
from core.sanitize import redact_url

FEEDBACK_DLQ_NAME = "feedback_queue.dlq"

logger = logging.getLogger("ai_worker")


def get_recent_feedback_reasons(limit: int = 5) -> list[str]:
    """Вибирає останні унікальні причини відхилень із PostgreSQL для пам'яті ШІ."""
    conn = None
    try:
        conn = psycopg2.connect(**settings.pg_connect_kwargs())
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
        logger.error(f"[FeedbackEngine] Failed to read feedback memory: {redact_url(err)}")
        return []
    finally:
        if conn is not None:
            conn.close()


def _normalize_status(raw_status) -> str:
    if isinstance(raw_status, str):
        cleaned = raw_status.strip().lower()
        if cleaned and len(cleaned) <= 20:
            return cleaned
    return "rejected"


def _persist_feedback(
    pg_url: str | None,
    job_title: str,
    reason: str,
    status: str = "rejected",
    conn=None,
):
    owned_conn = conn is None
    if conn is not None:
        db_conn = conn
    elif pg_url:
        db_conn = psycopg2.connect(pg_url, connect_timeout=10, options="-c statement_timeout=15000")
    else:
        db_conn = psycopg2.connect(**settings.pg_connect_kwargs())
    cur = db_conn.cursor()
    normalized_status = _normalize_status(status)
    try:
        cur.execute(
            """
            INSERT INTO user_feedback (job_title, reason, status)
            VALUES (%s, %s, %s);
            """,
            (job_title, reason, normalized_status),
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


def _route_feedback_to_dlq(ch, delivery_tag, body: bytes, reason: str) -> None:
    if not is_channel_open(ch):
        return
    if publish_to_dlq(ch, FEEDBACK_DLQ_NAME, "feedback_queue", body, reason):
        safe_ack(ch, delivery_tag)
    else:
        safe_nack(ch, delivery_tag, requeue=False)


def start_feedback_listener_thread(stop_event: threading.Event | None = None):
    """Запускає фоновий потік, який слухає feedback_queue з RabbitMQ із автоматичним перепідключенням."""
    def run():
        backoff = 1.0
        while stop_event is None or not stop_event.is_set():
            connection = None
            try:
                params = settings.rmq_connection_parameters()
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.queue_declare(queue="feedback_queue", durable=True)
                channel.queue_declare(queue=FEEDBACK_DLQ_NAME, durable=True)
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
                        status = _normalize_status(data.get("status"))
                    except Exception as e:
                        logger.error(f"[FeedbackEngine] Invalid feedback payload: {e}")
                        _route_feedback_to_dlq(ch, method.delivery_tag, body, f"invalid_payload: {e}")
                        return

                    try:
                        logger.info(f"[FeedbackEngine] Captured human rejection for '{job_title}': '{reason}'")
                        _persist_feedback(None, job_title, reason, status=status)
                        safe_ack(ch, method.delivery_tag)
                        logger.info("[FeedbackEngine] Stored feedback in PostgreSQL successfully.")
                    except Exception as e:
                        logger.error(f"[FeedbackEngine] Error storing feedback in PostgreSQL: {redact_url(e)}")
                        if not getattr(method, "redelivered", False):
                            safe_nack(ch, method.delivery_tag, requeue=True)
                        else:
                            _route_feedback_to_dlq(ch, method.delivery_tag, body, f"db_error: {e}")

                channel.basic_consume(queue="feedback_queue", on_message_callback=callback)
                if stop_event is not None:
                    while not stop_event.is_set():
                        connection.process_data_events(time_limit=1.0)
                        if not is_channel_open(channel) or not is_connection_open(connection):
                            break
                    if is_channel_open(channel):
                        try:
                            channel.stop_consuming()
                        except Exception:
                            pass
                else:
                    channel.start_consuming()

            except Exception as e:
                if stop_event is not None and stop_event.is_set():
                    break
                logger.error(
                    f"[FeedbackEngine] Feedback listener disconnected: {redact_url(e)}. Reconnecting in {backoff:.1f}s..."
                )
                if stop_event is not None:
                    stop_event.wait(backoff)
                else:
                    time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            finally:
                if is_connection_open(connection):
                    try:
                        connection.close()
                    except Exception:
                        pass

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def drain_feedback_queue(amqp_url: str | None = None, db_url: str | None = None) -> int:
    """Synchronously drains all pending messages from feedback_queue into PostgreSQL."""
    processed = 0
    connection = None
    pg_conn = None
    try:
        if amqp_url:
            params = build_pika_parameters(amqp_url)
        else:
            params = settings.rmq_connection_parameters()
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="feedback_queue", durable=True)
        channel.queue_declare(queue=FEEDBACK_DLQ_NAME, durable=True)

        def _open_pg_conn():
            if db_url:
                return psycopg2.connect(db_url, connect_timeout=10, options="-c statement_timeout=15000")
            return psycopg2.connect(**settings.pg_connect_kwargs())

        def _close_pg_conn():
            nonlocal pg_conn
            if pg_conn is not None:
                try:
                    pg_conn.close()
                except Exception:
                    pass
                finally:
                    pg_conn = None

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
                status = _normalize_status(data.get("status"))
            except Exception as e:
                logger.error(f"[FeedbackEngine] Invalid drained feedback payload: {redact_url(e)}")
                _route_feedback_to_dlq(channel, method_frame.delivery_tag, body, f"invalid_payload: {e}")
                continue

            try:
                if pg_conn is None or getattr(pg_conn, "closed", 0):
                    pg_conn = _open_pg_conn()
                _persist_feedback(db_url, job_title, reason, status=status, conn=pg_conn)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                logger.warning(
                    f"[FeedbackEngine] PostgreSQL connection error while draining; reconnecting once: "
                    f"{redact_url(db_err)}"
                )
                _close_pg_conn()
                try:
                    pg_conn = _open_pg_conn()
                    _persist_feedback(db_url, job_title, reason, status=status, conn=pg_conn)
                except Exception as retry_err:
                    logger.error(
                        f"[FeedbackEngine] Retry failed draining feedback message: {redact_url(retry_err)}"
                    )
                    if isinstance(retry_err, (psycopg2.OperationalError, psycopg2.InterfaceError)):
                        _close_pg_conn()
                    _route_feedback_to_dlq(
                        channel,
                        method_frame.delivery_tag,
                        body,
                        f"drain_db_error: {retry_err}",
                    )
                    continue
            except Exception as e:
                logger.error(f"[FeedbackEngine] Error draining feedback message: {redact_url(e)}")
                _route_feedback_to_dlq(channel, method_frame.delivery_tag, body, f"drain_error: {e}")
                continue

            safe_ack(channel, method_frame.delivery_tag)
            processed += 1
            logger.info(f"[FeedbackEngine] Drained feedback for '{job_title}': '{reason}'")
    except Exception as e:
        logger.error(f"[FeedbackEngine] Failed to drain feedback_queue: {redact_url(e)}")
    finally:
        if pg_conn is not None:
            try:
                pg_conn.close()
            except Exception:
                pass
        if is_connection_open(connection):
            try:
                connection.close()
            except Exception:
                pass
    return processed
