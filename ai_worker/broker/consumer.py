import json
import logging
import time
import pika

logger = logging.getLogger("ai_worker")


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
        self._connection_params = connection_params
        self.connection = None
        self.channel = None

    def connect(self):
        if self._connection_params is not None:
            params = self._connection_params
        else:
            params = pika.URLParameters(self.amqp_url)
            params.heartbeat = 600
            params.blocked_connection_timeout = 300
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        # Оголошуємо чергу для вхідних вакансій
        self.channel.queue_declare(queue=self.queue_name, durable=True)

        # Оголошуємо чергу для готових пропозицій, які читатиме Go Telegram Bot
        self.channel.queue_declare(queue="review_queue", durable=True)

        self.channel.basic_qos(prefetch_count=1)
        logger.info(f"Connected to RabbitMQ. Listening on '{self.queue_name}', publishing to 'review_queue'")

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
            )
        )
        logger.info(f"[+] Proposal published to RabbitMQ queue: '{queue_name}'")

    def start_consuming(self, message_handler):
        def callback(ch, method, properties, body):
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception as err:
                logger.error(f"Failed to decode message JSON: {err}", exc_info=True)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            if not isinstance(payload, dict):
                logger.error("Received non-object JSON payload on queue; discarding.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            if payload.get("_batch_eof") is True:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"Ignored batch EOF marker in daemon mode on '{self.queue_name}'.")
                return

            try:
                title = payload.get("Title", "No Title")
                logger.info(f"Received vacancy: '{title}'")

                # Викликаємо пайплайн агентів, передаючи метод для публікації результату
                message_handler(payload, self.publish_review)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info("Message processed and ACK sent.")

            except Exception as err:
                logger.error(f"Failed to process message: {err}", exc_info=True)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback
        )

        logger.info("Waiting for messages from RabbitMQ. To exit press CTRL+C")
        self.channel.start_consuming()

    def publish_batch_eof(self, queue_name: str = "review_queue"):
        """Publishes a batch EOF marker so downstream consumers know the batch is complete."""
        self.publish_review({"_batch_eof": True}, queue_name=queue_name)
        logger.info(f"[+] Batch EOF marker published to RabbitMQ queue: '{queue_name}'")

    def consume_batch(self, message_handler, idle_timeout: float = 20.0, stop_event=None):
        """Drains queue until _batch_eof marker or idle_timeout seconds without messages."""
        last_activity = time.monotonic()
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
                    self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
                    last_activity = time.monotonic()
                    continue

                if payload.get("_batch_eof") is True:
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    logger.info(f"Received batch EOF marker on '{self.queue_name}'.")
                    break

                try:
                    title = payload.get("Title", "No Title")
                    logger.info(f"Received vacancy: '{title}'")
                    message_handler(payload, self.publish_review)
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    logger.info("Message processed and ACK sent.")
                except Exception as err:
                    logger.error(f"Failed to process message: {err}", exc_info=True)
                    self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

                last_activity = time.monotonic()
        finally:
            try:
                self.channel.cancel()
            except Exception:
                pass
        self.publish_batch_eof(queue_name="review_queue")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")
