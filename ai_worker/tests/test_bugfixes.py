import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

AI_WORKER_DIR = Path(__file__).resolve().parent.parent
if str(AI_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(AI_WORKER_DIR))

from broker.consumer import RabbitMQConsumer
from broker import feedback_consumer
from config.config import Config
from core.llm import call_with_retry, is_transient_error
from core.logger import setup_logger
from rag import indexer


class TestConfigAndLoggerFixes(unittest.TestCase):
    def test_url_encodes_special_chars_and_handles_bad_timeout(self):
        with patch.dict(
            os.environ,
            {
                "PG_USER": "u",
                "PG_PASSWORD": "p@ss:word/1#2",
                "PG_HOST": "localhost",
                "PG_PORT": "5432",
                "PG_DB": "bidder_db",
                "RMQ_USER": "ru",
                "RMQ_PASS": "r@mq:pass/3",
                "RMQ_HOST": "localhost",
                "RMQ_PORT": "5672",
                "AI_BATCH_IDLE_TIMEOUT": "not-a-number",
            },
        ):
            cfg = Config()
            self.assertEqual(
                cfg.database_url(),
                "postgresql://u:p%40ss%3Aword%2F1%232@localhost:5432/bidder_db",
            )
            self.assertEqual(
                cfg.rabbitmq_url(),
                "amqp://ru:r%40mq%3Apass%2F3@localhost:5672/",
            )
            self.assertEqual(cfg.batch_idle_timeout, 20.0)

    def test_setup_logger_without_directory_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                logger = setup_logger("flat.log")
                self.assertIsNotNone(logger)
            finally:
                os.chdir(cwd)


class TestDaemonBatchEOFIgnored(unittest.TestCase):
    def test_start_consuming_acks_and_skips_batch_eof(self):
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")
        consumer.channel = MagicMock()

        captured = {}

        def fake_basic_consume(queue, on_message_callback):
            captured["cb"] = on_message_callback

        consumer.channel.basic_consume.side_effect = fake_basic_consume
        handler = MagicMock()
        consumer.start_consuming(handler)

        ch = MagicMock()
        method = SimpleNamespace(delivery_tag=42)
        captured["cb"](ch, method, None, json.dumps({"_batch_eof": True}).encode("utf-8"))

        handler.assert_not_called()
        ch.basic_ack.assert_called_once_with(delivery_tag=42)


class TestFeedbackSQLAndRetry(unittest.TestCase):
    def test_get_recent_feedback_reasons_orders_by_created_at_desc(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("Wrong stack",), ("Too senior",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("broker.feedback_consumer.psycopg2.connect", return_value=mock_conn):
            reasons = feedback_consumer.get_recent_feedback_reasons(limit=5)

        self.assertEqual(reasons, ["Wrong stack", "Too senior"])
        executed_sql = mock_cur.execute.call_args[0][0]
        self.assertIn("ORDER BY MAX(created_at) DESC", executed_sql)
        mock_conn.close.assert_called_once()

    def test_call_with_retry_retries_transient_then_succeeds(self):
        attempts = {"n": 0}

        def flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("503 Service Unavailable")
            return "ok"

        with patch("core.llm.time.sleep"):
            result = call_with_retry(flaky, operation="test", max_attempts=3, base_delay=0.01)

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["n"], 3)

    def test_call_with_retry_does_not_retry_non_transient(self):
        attempts = {"n": 0}

        def permanent():
            attempts["n"] += 1
            raise ValueError("bad schema")

        with patch("core.llm.time.sleep") as mock_sleep:
            with self.assertRaises(ValueError):
                call_with_retry(permanent, operation="test", max_attempts=3, base_delay=0.01)

        self.assertEqual(attempts["n"], 1)
        mock_sleep.assert_not_called()
        self.assertFalse(is_transient_error(ValueError("bad schema")))


class TestIndexerAtomicity(unittest.TestCase):
    def test_indexer_does_not_truncate_db_if_embedding_fails(self):
        fake_chunks = [
            {"project": "SplitCore", "category": "DB", "content": "pgxpool"},
            {"project": "SplitCore", "category": "MQ", "content": "rabbitmq"},
        ]

        with patch.object(indexer.settings, "profile_data", {"knowledge_chunks": fake_chunks}), \
             patch.object(indexer.embedder, "embed_text", side_effect=[[0.1] * 768, RuntimeError("429 quota")]), \
             patch("rag.indexer.psycopg2.connect") as mock_connect:
            with self.assertRaises(RuntimeError):
                indexer.index_knowledge_base()
            # DB must never be touched or truncated if embedding fails
            mock_connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
