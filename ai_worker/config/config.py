import os
import json
from pathlib import Path
from dotenv import load_dotenv

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

# Check potential .env locations
ENV_PATH = PROJECT_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    load_dotenv()


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

        # 5. Candidate Profile (Dynamic JSON)
        self.profile_data = self._load_profile()

    def _load_profile(self) -> dict:
        paths = [
            PROJECT_ROOT / "candidate_profile.json",
            Path("/app/candidate_profile.json"),
            Path("candidate_profile.json"),
            CURRENT_DIR.parent / "candidate_profile.json",
        ]
        for p in paths:
            if p.exists():
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    pass
        return {}

    def rabbitmq_url(self) -> str:
        return f"amqp://{self.rmq_user}:{self.rmq_pass}@{self.rmq_host}:{self.rmq_port}/"

    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Config()
