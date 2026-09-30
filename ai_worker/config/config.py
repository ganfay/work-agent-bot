import os
from dotenv import load_dotenv

load_dotenv("../.env")


class Config:
    def __init__(self):
        # 1. Database
        self.db_user = os.getenv("PG_USER", "postgres")
        self.db_password = os.getenv("PG_PASSWORD", "")
        self.db_host = os.getenv("PG_HOST", "localhost")
        self.db_port = os.getenv("PG_PORT", "5432")
        self.db_name = os.getenv("PG_DB", "bidder_db")

        # 2. RabbitMQ
        self.rmq_user = os.getenv("RMQ_USER", "guest")
        self.rmq_pass = os.getenv("RMQ_PASS") or os.getenv("RMQ_PASSWORD", "guest")
        self.rmq_host = os.getenv("RMQ_HOST", "localhost")
        self.rmq_port = os.getenv("RMQ_PORT", "5672")

        # 3. AI Settings
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "")

        # 4. Worker Settings
        self.queue_name = "new_vacancies"
        self.log_path = "logs/ai_worker.log"

    def rabbitmq_url(self) -> str:
        return f"amqp://{self.rmq_user}:{self.rmq_pass}@{self.rmq_host}:{self.rmq_port}/"

    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Config()
