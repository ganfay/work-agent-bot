import sys
from broker.consumer import RabbitMQConsumer
from config.config import settings
from core.logger import setup_logger

logger = setup_logger(settings.log_path)


def process_vacancy(job: dict):
    title = job.get("Title")
    link = job.get("Link")
    description = job.get("Description", "")
    
    logger.info(f"--> [AI PIPELINE] Starting analysis for: {title}")
    logger.info(f"--> [AI PIPELINE] Link: {link}")
    logger.info(f"--> [AI PIPELINE] Description preview: {description[:100]}...")


def main():
    logger.info("Starting AI Worker Service...")

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
