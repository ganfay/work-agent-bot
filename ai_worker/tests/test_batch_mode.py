import json
import os
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

AI_WORKER_DIR = Path(__file__).resolve().parent.parent
if str(AI_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(AI_WORKER_DIR))

from config.config import Config
from broker.consumer import RabbitMQConsumer
from broker import feedback_consumer


class TestConfigBatchMode(unittest.TestCase):
    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RUN_ONCE", None)
            os.environ.pop("BATCH_IDLE_TIMEOUT", None)
            cfg = Config()
            self.assertFalse(cfg.run_once)
            self.assertEqual(cfg.batch_idle_timeout, 20.0)

    def test_run_once_overrides(self):
        with patch.dict(os.environ, {"RUN_ONCE": "true", "BATCH_IDLE_TIMEOUT": "12.5"}):
            cfg = Config()
            self.assertTrue(cfg.run_once)
            self.assertEqual(cfg.batch_idle_timeout, 12.5)

    def test_duration_suffix_and_ai_specific_override(self):
        with patch.dict(os.environ, {"AI_BATCH_IDLE_TIMEOUT": "15s", "BATCH_IDLE_TIMEOUT": "90"}):
            cfg = Config()
            self.assertEqual(cfg.batch_idle_timeout, 15.0)


class TestRabbitMQConsumerBatch(unittest.TestCase):
    def _make_consumer(self):
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")
        consumer.channel = MagicMock()
        consumer.connection = MagicMock()
        consumer.connection.is_closed = False
        return consumer

    def test_consume_batch_processes_messages_and_stops_on_eof(self):
        consumer = self._make_consumer()
        m1 = SimpleNamespace(delivery_tag=1)
        m_eof = SimpleNamespace(delivery_tag=2)

        consumer.channel.consume.return_value = iter([
            (m1, None, json.dumps({"Title": "Go Dev", "Link": "https://x"}).encode("utf-8")),
            (m_eof, None, json.dumps({"_batch_eof": True}).encode("utf-8")),
        ])

        handled = []

        def handler(payload, publish_fn):
            handled.append(payload["Title"])
            publish_fn({"title": payload["Title"]})

        consumer.consume_batch(handler, idle_timeout=5.0)

        self.assertEqual(handled, ["Go Dev"])
        consumer.channel.basic_ack.assert_any_call(delivery_tag=1)
        consumer.channel.basic_ack.assert_any_call(delivery_tag=2)
        consumer.channel.cancel.assert_called_once()

        # Verify review_queue received the proposal AND the _batch_eof marker
        publish_calls = consumer.channel.basic_publish.call_args_list
        self.assertEqual(len(publish_calls), 2)
        last_body = json.loads(publish_calls[-1].kwargs["body"].decode("utf-8"))
        self.assertEqual(last_body, {"_batch_eof": True})

    def test_consume_batch_stops_on_idle_timeout(self):
        consumer = self._make_consumer()
        # Simulate inactivity ticks
        consumer.channel.consume.return_value = iter([
            (None, None, None),
            (None, None, None),
            (None, None, None),
        ])

        # monotonic advances by 0.6s per call -> after 2 ticks > 1.0s idle_timeout
        with patch("broker.consumer.time.monotonic", side_effect=[0.0, 0.6, 1.2]):
            consumer.consume_batch(lambda p, f: None, idle_timeout=1.0)

        consumer.channel.cancel.assert_called_once()
        publish_calls = consumer.channel.basic_publish.call_args_list
        self.assertEqual(len(publish_calls), 1)
        last_body = json.loads(publish_calls[0].kwargs["body"].decode("utf-8"))
        self.assertEqual(last_body, {"_batch_eof": True})

    def test_consume_batch_nacks_bad_json_and_handler_error(self):
        consumer = self._make_consumer()
        m_bad_json = SimpleNamespace(delivery_tag=10)
        m_bad_handler = SimpleNamespace(delivery_tag=11)
        m_eof = SimpleNamespace(delivery_tag=12)

        consumer.channel.consume.return_value = iter([
            (m_bad_json, None, b"{not-json"),
            (m_bad_handler, None, json.dumps({"Title": "Boom"}).encode("utf-8")),
            (m_eof, None, json.dumps({"_batch_eof": True}).encode("utf-8")),
        ])

        def failing_handler(payload, publish_fn):
            raise RuntimeError("gemini error")

        consumer.consume_batch(failing_handler, idle_timeout=5.0)

        consumer.channel.basic_nack.assert_any_call(delivery_tag=10, requeue=False)
        consumer.channel.basic_nack.assert_any_call(delivery_tag=11, requeue=False)
        consumer.channel.basic_ack.assert_called_once_with(delivery_tag=12)

    def test_consume_batch_does_not_publish_eof_on_interrupt(self):
        consumer = self._make_consumer()
        m1 = SimpleNamespace(delivery_tag=1)
        consumer.channel.consume.return_value = iter([
            (m1, None, json.dumps({"Title": "Go Dev"}).encode("utf-8")),
        ])

        def interrupting_handler(payload, publish_fn):
            raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            consumer.consume_batch(interrupting_handler, idle_timeout=5.0)

        consumer.channel.cancel.assert_called_once()
        consumer.channel.basic_publish.assert_not_called()


class TestDrainFeedbackQueue(unittest.TestCase):
    def test_drain_feedback_queue_persists_all_messages(self):
        m1 = SimpleNamespace(delivery_tag=1)
        m2 = SimpleNamespace(delivery_tag=2)

        mock_channel = MagicMock()
        mock_channel.basic_get.side_effect = [
            (m1, None, json.dumps({"job_title": "Job A", "reason": "Low pay"}).encode("utf-8")),
            (m2, None, json.dumps({"job_title": "Job B", "reason": "Wrong stack"}).encode("utf-8")),
            (None, None, None),
        ]
        mock_conn = MagicMock()
        mock_conn.is_closed = False
        mock_conn.channel.return_value = mock_channel

        mock_cur = MagicMock()
        mock_pg_conn = MagicMock()
        mock_pg_conn.cursor.return_value = mock_cur

        with patch("broker.feedback_consumer.pika.BlockingConnection", return_value=mock_conn), \
             patch("broker.feedback_consumer.psycopg2.connect", return_value=mock_pg_conn):
            count = feedback_consumer.drain_feedback_queue("amqp://u:p@h:5672/", "postgresql://x")

        self.assertEqual(count, 2)
        self.assertEqual(mock_cur.execute.call_count, 2)
        mock_channel.basic_ack.assert_any_call(delivery_tag=1)
        mock_channel.basic_ack.assert_any_call(delivery_tag=2)
        mock_conn.close.assert_called_once()


class TestMainModes(unittest.TestCase):
    def test_main_run_once_drains_and_exits_cleanly(self):
        # Stub google.genai / agent modules if not installed in test env
        fake_agents = {}
        for mod_name, cls_name in [
            ("agents.analyzer", "VacancyAnalyzer"),
            ("agents.validator", "EvidenceValidator"),
            ("agents.drafter", "CoverLetterDrafter"),
            ("agents.critic", "CoverLetterCritic"),
        ]:
            if mod_name not in sys.modules:
                m = types.ModuleType(mod_name)
                setattr(m, cls_name, MagicMock())
                fake_agents[mod_name] = m
        if "rag.indexer" not in sys.modules:
            m = types.ModuleType("rag.indexer")
            m.index_knowledge_base = MagicMock()
            fake_agents["rag.indexer"] = m

        with patch.dict(sys.modules, fake_agents):
            import main as worker_main

            mock_consumer = MagicMock()
            with patch.object(worker_main, "ensure_embeddings_exist"), \
                 patch.object(worker_main, "init_agents"), \
                 patch.object(worker_main, "drain_feedback_queue") as mock_drain, \
                 patch.object(worker_main, "start_feedback_listener_thread") as mock_thread, \
                 patch.object(worker_main, "RabbitMQConsumer", return_value=mock_consumer), \
                 patch.object(worker_main.settings, "run_once", True), \
                 patch.object(worker_main.settings, "batch_idle_timeout", 7.0):
                worker_main.main()

            self.assertEqual(mock_drain.call_count, 2)
            mock_thread.assert_not_called()
            mock_consumer.connect.assert_called_once()
            mock_consumer.consume_batch.assert_called_once_with(
                message_handler=worker_main.process_vacancy,
                idle_timeout=7.0,
            )
            mock_consumer.start_consuming.assert_not_called()
            mock_consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
