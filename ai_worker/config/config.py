import os
import json
from pathlib import Path
from urllib.parse import quote
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
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite").strip() or "gemini-3.5-flash-lite"
        self.gemini_embedding_model = (
            os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001").strip() or "gemini-embedding-001"
        )

        # 4. Worker Settings
        self.queue_name = "new_vacancies"
        self.log_path = os.getenv("AI_LOG_PATH", "logs/ai_worker.log").strip() or "logs/ai_worker.log"
        self.run_once = os.getenv("RUN_ONCE", "false").strip().lower() in ("true", "1", "yes")
        self.batch_idle_timeout = self._parse_idle_timeout(
            os.getenv("AI_BATCH_IDLE_TIMEOUT") or os.getenv("BATCH_IDLE_TIMEOUT") or "20"
        )

        # 5. Candidate Profile (Dynamic JSON)
        self.profile_data = self._load_profile()

    @staticmethod
    def _parse_idle_timeout(raw: str, default: float = 20.0) -> float:
        cleaned = raw.strip()
        if cleaned.endswith("s"):
            cleaned = cleaned[:-1].strip()
        try:
            val = float(cleaned)
            return val if val > 0 else default
        except ValueError:
            return default

    def _load_profile(self) -> dict:
        paths = [
            PROJECT_ROOT / "candidate_profile.json",
            Path("/app/candidate_profile.json"),
            Path("candidate_profile.json"),
            CURRENT_DIR.parent / "candidate_profile.json",
        ]
        for p in paths:
            if not p.exists():
                continue
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError as err:
                raise ValueError(f"Invalid JSON in candidate profile '{p}': {err}") from err
            if not isinstance(data, dict):
                raise ValueError(
                    f"Candidate profile '{p}' must be a JSON object, got {type(data).__name__}"
                )
            return data
        return {}

    def rabbitmq_url(self) -> str:
        user = quote(self.rmq_user, safe="")
        password = quote(self.rmq_pass, safe="")
        return f"amqp://{user}:{password}@{self.rmq_host}:{self.rmq_port}/"

    def database_url(self) -> str:
        user = quote(self.db_user, safe="")
        password = quote(self.db_password, safe="")
        return f"postgresql://{user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def pg_connect_kwargs(self, connect_timeout: int = 10, statement_timeout_ms: int = 15000) -> dict:
        port = int(self.db_port) if str(self.db_port).isdigit() else self.db_port
        return {
            "host": self.db_host,
            "port": port,
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
            "connect_timeout": connect_timeout,
            "options": f"-c statement_timeout={statement_timeout_ms}",
        }

    def rmq_connection_kwargs(
        self,
        heartbeat: int = 600,
        blocked_connection_timeout: int = 300,
        connection_attempts: int = 3,
        retry_delay: float = 2.0,
        socket_timeout: float = 10.0,
    ) -> dict:
        port = int(self.rmq_port) if str(self.rmq_port).isdigit() else self.rmq_port
        return {
            "host": self.rmq_host,
            "port": port,
            "virtual_host": "/",
            "username": self.rmq_user,
            "password": self.rmq_pass,
            "heartbeat": heartbeat,
            "blocked_connection_timeout": blocked_connection_timeout,
            "connection_attempts": connection_attempts,
            "retry_delay": retry_delay,
            "socket_timeout": socket_timeout,
        }

    def rmq_connection_parameters(
        self,
        heartbeat: int = 600,
        blocked_connection_timeout: int = 300,
        connection_attempts: int = 3,
        retry_delay: float = 2.0,
        socket_timeout: float = 10.0,
    ):
        import pika

        kw = self.rmq_connection_kwargs(
            heartbeat=heartbeat,
            blocked_connection_timeout=blocked_connection_timeout,
            connection_attempts=connection_attempts,
            retry_delay=retry_delay,
            socket_timeout=socket_timeout,
        )
        return pika.ConnectionParameters(
            host=kw["host"],
            port=kw["port"],
            virtual_host=kw["virtual_host"],
            credentials=pika.PlainCredentials(kw["username"], kw["password"]),
            heartbeat=kw["heartbeat"],
            blocked_connection_timeout=kw["blocked_connection_timeout"],
            connection_attempts=kw["connection_attempts"],
            retry_delay=kw["retry_delay"],
            socket_timeout=kw["socket_timeout"],
        )


settings = Config()
