import json
import logging
import pika

logger = logging.getLogger("ai_worker")


class RabbitMQConsumer:
    def __init__(self, amqp_url: str, queue_name: str):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        params = pika.URLParameters(self.amqp_url)
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

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")
