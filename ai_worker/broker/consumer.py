from datetime import datetime, timezone
import json
import logging
import threading
import time
from urllib.parse import unquote, urlparse
import pika
import pika.exceptions

from core.sanitize import redact_url

logger = logging.getLogger("ai_worker")


def is_channel_open(ch) -> bool:
    return (
        ch is not None
        and getattr(ch, "is_open", None) is not False
        and getattr(ch, "is_closed", None) is not True
    )


def is_connection_open(conn) -> bool:
    return (
        conn is not None
        and getattr(conn, "is_open", None) is not False
        and getattr(conn, "is_closed", None) is not True
    )


def safe_ack(ch, delivery_tag) -> bool:
    if not is_channel_open(ch):
        return False
    try:
        ch.basic_ack(delivery_tag=delivery_tag)
        return True
    except Exception as err:
        logger.warning(f"Failed to ACK delivery_tag={delivery_tag}: {redact_url(err)}")
        return False


def safe_nack(ch, delivery_tag, requeue: bool = False) -> bool:
    if not is_channel_open(ch):
        return False
    try:
        ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        return True
    except Exception as err:
        logger.warning(f"Failed to NACK delivery_tag={delivery_tag}: {redact_url(err)}")
        return False


def build_pika_parameters(
    amqp_url: str,
    *,
    heartbeat: int = 600,
    blocked_connection_timeout: int = 300,
    connection_attempts: int = 3,
    retry_delay: float = 2.0,
    socket_timeout: float = 10.0,
) -> pika.ConnectionParameters:
    parsed = urlparse(amqp_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5672
    user = unquote(parsed.username) if parsed.username is not None else "guest"
    password = unquote(parsed.password) if parsed.password is not None else "guest"
    raw_path = parsed.path or "/"
    vhost = "/" if raw_path == "/" else unquote(raw_path.lstrip("/")) or "/"
    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=pika.PlainCredentials(user, password),
        heartbeat=heartbeat,
        blocked_connection_timeout=blocked_connection_timeout,
        connection_attempts=connection_attempts,
        retry_delay=retry_delay,
        socket_timeout=socket_timeout,
    )


def publish_to_dlq(
    ch,
    dlq_name: str,
    original_queue: str,
    raw_body: bytes,
    reason: str,
) -> bool:
    if not is_channel_open(ch):
        return False
    if isinstance(raw_body, bytes):
        text_body = raw_body.decode("utf-8", errors="replace")
    else:
        text_body = str(raw_body)
    try:
        decoded_payload = json.loads(text_body)
    except Exception:
        decoded_payload = text_body

    envelope = {
        "original_queue": original_queue,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "error_reason": redact_url(reason)[:1000],
        "payload": decoded_payload,
    }
    try:
        ch.basic_publish(
            exchange="",
            routing_key=dlq_name,
            body=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        return True
    except Exception as err:
        logger.error(f"Failed to publish message to DLQ '{dlq_name}': {redact_url(err)}")
        return False


class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str | None = None,
        queue_name: str = "new_vacancies",
        *,
        connection_params=None,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.dlq_name = f"{queue_name}.dlq"
        self._connection_params = connection_params
        self.connection = None
        self.channel = None

    def connect(self):
        if self._connection_params is not None:
            params = self._connection_params
        elif self.amqp_url:
            params = build_pika_parameters(self.amqp_url)
        else:
            raise ValueError("Either amqp_url or connection_params must be provided")

        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        # Оголошуємо чергу для вхідних вакансій (args не змінюємо для сумісності зі scraper)
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        # Оголошуємо DLQ для повідомлень, які не вдалося обробити
        self.channel.queue_declare(queue=self.dlq_name, durable=True)
        # Оголошуємо чергу для готових пропозицій, які читатиме Go Telegram Bot
        self.channel.queue_declare(queue="review_queue", durable=True)

        self.channel.basic_qos(prefetch_count=1)
        logger.info(
            f"Connected to RabbitMQ. Listening on '{self.queue_name}' "
            f"(DLQ: '{self.dlq_name}'), publishing to 'review_queue'"
        )

    def publish_review(self, payload: dict, queue_name: str = "review_queue"):
        """Публікує фінальний результат роботи 4 агентів у чергу review_queue."""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Зробити повідомлення стійким до перезапуску RabbitMQ (durable)
                content_type="application/json",
            ),
        )
        logger.info(f"[+] Proposal published to RabbitMQ queue: '{queue_name}'")

    def _route_failed_message(self, ch, delivery_tag, body: bytes, reason: str) -> None:
        if not is_channel_open(ch):
            logger.error(
                f"Channel closed while handling failed message (delivery_tag={delivery_tag}); "
                "skipping DLQ/NACK so broker redelivers on reconnect."
            )
            return
        if publish_to_dlq(ch, self.dlq_name, self.queue_name, body, reason):
            safe_ack(ch, delivery_tag)
        else:
            safe_nack(ch, delivery_tag, requeue=False)

    @staticmethod
    def _stop_channel_callback(ch) -> None:
        if not is_channel_open(ch):
            return
        try:
            if getattr(ch, "_queue_consumer_generator", None) is not None:
                ch.cancel()
            else:
                ch.stop_consuming()
        except Exception as err:
            logger.debug(f"Ignored error while stopping RabbitMQ channel: {redact_url(err)}")

    def request_stop(self) -> None:
        conn = self.connection
        ch = self.channel
        if is_connection_open(conn):
            try:
                conn.add_callback_threadsafe(lambda: self._stop_channel_callback(ch))
            except Exception as err:
                logger.debug(f"Failed to schedule RabbitMQ stop callback: {redact_url(err)}")

    def start_consuming(self, message_handler, stop_event: threading.Event | None = None):
        def callback(ch, method, properties, body):
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception as err:
                logger.error(f"Failed to decode message JSON: {err}", exc_info=True)
                self._route_failed_message(ch, method.delivery_tag, body, f"json_decode: {err}")
                return

            if not isinstance(payload, dict):
                logger.error("Received non-object JSON payload on queue; routing to DLQ.")
                self._route_failed_message(ch, method.delivery_tag, body, "payload_not_object")
                return

            if payload.get("_batch_eof") is True:
                safe_ack(ch, method.delivery_tag)
                logger.info(f"Ignored batch EOF marker in daemon mode on '{self.queue_name}'.")
                return

            try:
                title = payload.get("Title", "No Title")
                logger.info(f"Received vacancy: '{title}'")

                # Викликаємо пайплайн агентів, передаючи метод для публікації результату
                message_handler(payload, self.publish_review)

                safe_ack(ch, method.delivery_tag)
                logger.info("Message processed and ACK sent.")

            except Exception as err:
                logger.error(f"Failed to process message: {redact_url(err)}", exc_info=True)
                self._route_failed_message(ch, method.delivery_tag, body, f"handler_error: {err}")

        backoff = 1.0
        while stop_event is None or not stop_event.is_set():
            try:
                if (
                    self.channel is None
                    or not is_channel_open(self.channel)
                    or (self.connection is not None and not is_connection_open(self.connection))
                ):
                    self.connect()

                if stop_event is not None and stop_event.is_set():
                    break

                consumer_tag = self.channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=callback,
                )

                if stop_event is not None and stop_event.is_set():
                    if is_channel_open(self.channel):
                        try:
                            if isinstance(consumer_tag, str) and consumer_tag:
                                self.channel.stop_consuming(consumer_tag)
                            else:
                                self.channel.stop_consuming()
                        except Exception as err:
                            logger.debug(
                                f"Ignored error cancelling consumer during shutdown: {redact_url(err)}"
                            )
                    break

                backoff = 1.0
                logger.info("Waiting for messages from RabbitMQ. To exit press CTRL+C")
                self.channel.start_consuming()
                break
            except (pika.exceptions.AMQPError, OSError) as err:
                if stop_event is not None and stop_event.is_set():
                    break
                logger.error(
                    f"RabbitMQ consumer connection error: {redact_url(err)}. "
                    f"Reconnecting in {backoff:.1f}s..."
                )
                self.close()
                if stop_event is not None:
                    stop_event.wait(backoff)
                else:
                    time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    def publish_batch_eof(self, queue_name: str = "review_queue"):
        """Publishes a batch EOF marker so downstream consumers know the batch is complete."""
        self.publish_review({"_batch_eof": True}, queue_name=queue_name)
        logger.info(f"[+] Batch EOF marker published to RabbitMQ queue: '{queue_name}'")

    def consume_batch(self, message_handler, idle_timeout: float = 20.0, stop_event=None):
        """Drains queue until _batch_eof marker or idle_timeout seconds without messages."""
        if stop_event is not None and stop_event.is_set():
            logger.info("Batch consumer stop requested before draining.")
            return

        last_activity = time.monotonic()
        stopped_early = False
        logger.info(
            f"Batch mode: draining '{self.queue_name}' (idle_timeout={idle_timeout}s)..."
        )
        try:
            for method_frame, properties, body in self.channel.consume(
                queue=self.queue_name,
                inactivity_timeout=1.0,
            ):
                if stop_event is not None and stop_event.is_set():
                    logger.info("Batch consumer stop requested.")
                    stopped_early = True
                    break

                if method_frame is None:
                    if time.monotonic() - last_activity >= idle_timeout:
                        logger.info(
                            f"Idle timeout ({idle_timeout}s) reached on '{self.queue_name}'. Draining complete."
                        )
                        break
                    continue

                try:
                    payload = json.loads(body.decode("utf-8"))
                    if not isinstance(payload, dict):
                        raise ValueError("message payload must be a JSON object")
                except Exception as err:
                    logger.error(f"Failed to decode message JSON: {err}", exc_info=True)
                    self._route_failed_message(
                        self.channel,
                        method_frame.delivery_tag,
                        body,
                        f"json_decode: {err}",
                    )
                    last_activity = time.monotonic()
                    continue

                if payload.get("_batch_eof") is True:
                    safe_ack(self.channel, method_frame.delivery_tag)
                    logger.info(f"Received batch EOF marker on '{self.queue_name}'.")
                    break

                try:
                    title = payload.get("Title", "No Title")
                    logger.info(f"Received vacancy: '{title}'")
                    message_handler(payload, self.publish_review)
                    safe_ack(self.channel, method_frame.delivery_tag)
                    logger.info("Message processed and ACK sent.")
                except Exception as err:
                    logger.error(f"Failed to process message: {redact_url(err)}", exc_info=True)
                    self._route_failed_message(
                        self.channel,
                        method_frame.delivery_tag,
                        body,
                        f"handler_error: {err}",
                    )

                if stop_event is not None and stop_event.is_set():
                    logger.info("Batch consumer stop requested.")
                    stopped_early = True
                    break

                last_activity = time.monotonic()
        finally:
            if is_channel_open(self.channel):
                try:
                    self.channel.cancel()
                except Exception:
                    pass

        if stopped_early or (stop_event is not None and stop_event.is_set()):
            logger.info("Batch consumer stopped early; skipping _batch_eof publication.")
            return

        self.publish_batch_eof(queue_name="review_queue")

    def close(self):
        try:
            if is_channel_open(self.channel):
                try:
                    self.channel.close()
                except Exception:
                    pass
            if is_connection_open(self.connection):
                try:
                    self.connection.close()
                    logger.info("RabbitMQ connection closed.")
                except Exception as err:
                    logger.warning(f"Ignored error closing RabbitMQ connection: {redact_url(err)}")
        finally:
            self.channel = None
            self.connection = None
