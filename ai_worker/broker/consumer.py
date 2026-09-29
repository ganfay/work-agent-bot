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

        self.channel.queue_declare(queue=self.queue_name, durable=True)

        self.channel.basic_qos(prefetch_count=1)
        logger.info(f"Connected to RabbitMQ on queue: '{self.queue_name}'")

    def start_consuming(self, message_handler):
        
        def callback(ch, method, properties, body):
            try:
                payload = json.loads(body.decode("utf-8"))

                title = payload.get("Title", "No Title")
                logger.info(f"Received vacancy: '{title}'")

                message_handler(payload)

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
