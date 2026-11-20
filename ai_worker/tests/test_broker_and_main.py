import json
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pika.exceptions
import psycopg2

AI_WORKER_DIR = Path(__file__).resolve().parent.parent
if str(AI_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(AI_WORKER_DIR))

from broker.consumer import RabbitMQConsumer
from broker import feedback_consumer
import main as worker_main


class TestRabbitMQConsumerHardening(unittest.TestCase):
    def test_connect_declares_dlq_without_altering_primary_queue_args(self):
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn.channel.return_value = mock_channel

        consumer = RabbitMQConsumer("amqp://user:p%40ss@rmq:5672/vhost", "new_vacancies")
        with patch("broker.consumer.pika.BlockingConnection", return_value=mock_conn) as mock_bc:
            consumer.connect()

        params = mock_bc.call_args[0][0]
        self.assertEqual(params.host, "rmq")
        self.assertEqual(params.credentials.username, "user")
        self.assertEqual(params.credentials.password, "p@ss")
        self.assertEqual(params.virtual_host, "vhost")

        declare_calls = mock_channel.queue_declare.call_args_list
        self.assertEqual(len(declare_calls), 3)
        self.assertEqual(declare_calls[0].kwargs, {"queue": "new_vacancies", "durable": True})
        self.assertEqual(declare_calls[1].kwargs, {"queue": "new_vacancies.dlq", "durable": True})
        self.assertEqual(declare_calls[2].kwargs, {"queue": "review_queue", "durable": True})

    def test_start_consuming_dlq_and_closed_channel_guard_and_reconnect(self):
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")
        consumer.channel = MagicMock()

        captured = {}

        def fake_basic_consume(queue, on_message_callback):
            captured["cb"] = on_message_callback

        consumer.channel.basic_consume.side_effect = fake_basic_consume
        consumer.start_consuming(lambda p, f: (_ for _ in ()).throw(RuntimeError("boom")))

        # Bad JSON -> DLQ + ACK
        ch_open = MagicMock()
        ch_open.is_open = True
        ch_open.is_closed = False
        captured["cb"](ch_open, SimpleNamespace(delivery_tag=7), None, b"{bad")
        ch_open.basic_publish.assert_called_once()
        self.assertEqual(ch_open.basic_publish.call_args.kwargs["routing_key"], "new_vacancies.dlq")
        ch_open.basic_ack.assert_called_once_with(delivery_tag=7)

        # Closed channel -> no basic_ack / basic_nack / basic_publish
        ch_closed = MagicMock()
        ch_closed.is_open = False
        ch_closed.is_closed = True
        captured["cb"](ch_closed, SimpleNamespace(delivery_tag=8), None, json.dumps({"Title": "Go"}).encode("utf-8"))
        ch_closed.basic_publish.assert_not_called()
        ch_closed.basic_ack.assert_not_called()
        ch_closed.basic_nack.assert_not_called()

        # Reconnect loop stops when stop_event is set
        stop_ev = threading.Event()
        consumer2 = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")
        attempts = {"n": 0}

        def fail_connect():
            attempts["n"] += 1
            if attempts["n"] >= 2:
                stop_ev.set()
            raise pika.exceptions.AMQPConnectionError("lost")

        with patch.object(consumer2, "connect", side_effect=fail_connect):
            consumer2.start_consuming(MagicMock(), stop_event=stop_ev)
        self.assertEqual(attempts["n"], 2)

    def test_consume_batch_stop_event_skips_eof_and_close_resets_state(self):
        stop_ev = threading.Event()
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")
        consumer.channel = MagicMock()
        consumer.connection = MagicMock()

        # Mid-loop stop_event set by handler -> no _batch_eof published
        m1 = SimpleNamespace(delivery_tag=1)
        consumer.channel.consume.return_value = iter([
            (m1, None, json.dumps({"Title": "Go Dev"}).encode("utf-8")),
        ])

        def stop_handler(payload, publish_fn):
            stop_ev.set()

        consumer.consume_batch(stop_handler, idle_timeout=5.0, stop_event=stop_ev)
        consumer.channel.basic_publish.assert_not_called()

        # close() swallows exceptions and resets references
        consumer.channel.close.side_effect = RuntimeError("close err")
        consumer.connection.close.side_effect = RuntimeError("conn close err")
        consumer.close()
        self.assertIsNone(consumer.channel)
        self.assertIsNone(consumer.connection)

    def test_start_consuming_exits_when_stop_event_fires_during_connect(self):
        stop_ev = threading.Event()
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")

        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_channel.is_closed = False
        mock_channel._queue_consumer_generator = None

        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_conn.is_closed = False
        mock_conn.channel.return_value = mock_channel

        def connect_while_stopping(*args, **kwargs):
            # Signal handler fires while self.connection is still None inside connect()
            stop_ev.set()
            consumer.request_stop()
            return mock_conn

        with patch("broker.consumer.pika.BlockingConnection", side_effect=connect_while_stopping):
            consumer.start_consuming(MagicMock(), stop_event=stop_ev)

        mock_channel.basic_consume.assert_not_called()
        mock_channel.start_consuming.assert_not_called()

    def test_start_consuming_exits_when_stop_event_fires_between_basic_consume_and_start_consuming(self):
        stop_ev = threading.Event()
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost:5672/", "new_vacancies")

        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_channel.is_closed = False
        mock_channel._queue_consumer_generator = None

        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_conn.is_closed = False
        consumer.connection = mock_conn
        consumer.channel = mock_channel

        def stop_during_basic_consume(*args, **kwargs):
            stop_ev.set()
            consumer.request_stop()
            return "ctag-1"

        mock_channel.basic_consume.side_effect = stop_during_basic_consume

        consumer.start_consuming(MagicMock(), stop_event=stop_ev)

        mock_channel.basic_consume.assert_called_once()
        mock_conn.add_callback_threadsafe.assert_called_once()
        mock_channel.stop_consuming.assert_called_once_with("ctag-1")
        mock_channel.start_consuming.assert_not_called()


class TestFeedbackConsumerHardening(unittest.TestCase):
    def test_status_normalization(self):
        self.assertEqual(feedback_consumer._normalize_status("  APPROVED "), "approved")
        self.assertEqual(feedback_consumer._normalize_status("x" * 21), "rejected")
        self.assertEqual(feedback_consumer._normalize_status(None), "rejected")

    def test_listener_thread_stop_event_and_dlq_routing(self):
        stop_ev = threading.Event()
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn.channel.return_value = mock_channel

        captured = {}

        def fake_consume(queue, on_message_callback):
            captured["cb"] = on_message_callback

        mock_channel.basic_consume.side_effect = fake_consume

        def fake_process_events(time_limit=1.0):
            # Trigger invalid payload, first-time DB error (requeue), and redelivered DB error (DLQ)
            cb = captured["cb"]
            cb(mock_channel, SimpleNamespace(delivery_tag=1, redelivered=False), None, b"bad-json")
            with patch("broker.feedback_consumer._persist_feedback", side_effect=RuntimeError("db down")):
                cb(
                    mock_channel,
                    SimpleNamespace(delivery_tag=2, redelivered=False),
                    None,
                    json.dumps({"job_title": "Go", "reason": "Pay", "status": "rejected"}).encode("utf-8"),
                )
                cb(
                    mock_channel,
                    SimpleNamespace(delivery_tag=3, redelivered=True),
                    None,
                    json.dumps({"job_title": "Go", "reason": "Pay", "status": "rejected"}).encode("utf-8"),
                )
            stop_ev.set()

        mock_conn.process_data_events.side_effect = fake_process_events

        with patch("broker.feedback_consumer.pika.BlockingConnection", return_value=mock_conn):
            t = feedback_consumer.start_feedback_listener_thread(stop_event=stop_ev)
            t.join(timeout=3.0)

        self.assertFalse(t.is_alive())
        mock_channel.basic_nack.assert_any_call(delivery_tag=2, requeue=True)
        dlq_publishes = [
            c for c in mock_channel.basic_publish.call_args_list
            if c.kwargs.get("routing_key") == "feedback_queue.dlq"
        ]
        self.assertEqual(len(dlq_publishes), 2)
        mock_channel.basic_ack.assert_any_call(delivery_tag=1)
        mock_channel.basic_ack.assert_any_call(delivery_tag=3)

    def test_feedback_listener_skips_basic_consume_when_stop_event_fires_during_connect(self):
        stop_ev = threading.Event()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_channel.is_closed = False
        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_conn.is_closed = False
        mock_conn.channel.return_value = mock_channel

        def connect_and_stop(*args, **kwargs):
            stop_ev.set()
            return mock_conn

        with patch("broker.feedback_consumer.pika.BlockingConnection", side_effect=connect_and_stop):
            t = feedback_consumer.start_feedback_listener_thread(stop_event=stop_ev)
            t.join(timeout=3.0)

        self.assertFalse(t.is_alive())
        mock_channel.basic_consume.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_drain_feedback_queue_resets_pg_conn_on_operational_error(self):
        m1 = SimpleNamespace(delivery_tag=1)
        m2 = SimpleNamespace(delivery_tag=2)
        mock_channel = MagicMock()
        mock_channel.basic_get.side_effect = [
            (m1, None, json.dumps({"job_title": "A", "reason": "R1"}).encode("utf-8")),
            (m2, None, json.dumps({"job_title": "B", "reason": "R2"}).encode("utf-8")),
            (None, None, None),
        ]
        mock_rmq_conn = MagicMock()
        mock_rmq_conn.channel.return_value = mock_channel

        broken_pg = MagicMock()
        broken_pg.closed = 0
        healthy_pg = MagicMock()
        healthy_pg.closed = 0

        persist_calls = {"n": 0}

        def fake_persist(pg_url, title, reason, status="rejected", conn=None):
            persist_calls["n"] += 1
            if persist_calls["n"] == 1:
                raise psycopg2.OperationalError("connection closed")

        with patch("broker.feedback_consumer.pika.BlockingConnection", return_value=mock_rmq_conn), \
             patch("broker.feedback_consumer.psycopg2.connect", side_effect=[broken_pg, healthy_pg]), \
             patch("broker.feedback_consumer._persist_feedback", side_effect=fake_persist):
            count = feedback_consumer.drain_feedback_queue("amqp://u:p@h:5672/", "postgresql://x")

        self.assertEqual(count, 2)
        broken_pg.close.assert_called_once()
        healthy_pg.close.assert_called_once()
        mock_channel.basic_ack.assert_any_call(delivery_tag=1)
        mock_channel.basic_ack.assert_any_call(delivery_tag=2)


class TestMainHardening(unittest.TestCase):
    def test_ensure_embeddings_exist_fail_fast_after_retries(self):
        with patch("main.psycopg2.connect", side_effect=psycopg2.OperationalError("db starting")), \
             patch("main.time.sleep") as mock_sleep:
            with self.assertRaises(psycopg2.OperationalError):
                worker_main.ensure_embeddings_exist(max_attempts=3, base_delay=0.01)
        self.assertEqual(mock_sleep.call_count, 2)

        # Indexing error must also propagate
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        with patch("main.psycopg2.connect", return_value=mock_conn), \
             patch("main.index_knowledge_base", side_effect=ValueError("bad chunk")):
            with self.assertRaises(ValueError):
                worker_main.ensure_embeddings_exist(max_attempts=1)
        mock_conn.close.assert_called_once()

    def test_init_and_close_agents_shared_client(self):
        mock_client = MagicMock()
        with patch.object(worker_main.settings, "gemini_api_key", "key-123"), \
             patch("main.create_genai_client", return_value=mock_client) as mock_ctor, \
             patch("main.VacancyAnalyzer") as mock_an, \
             patch("main.EvidenceValidator") as mock_va, \
             patch("main.CoverLetterDrafter") as mock_dr, \
             patch("main.CoverLetterCritic") as mock_cr:
            worker_main.init_agents()
            mock_ctor.assert_called_once_with("key-123")
            for cls_mock in (mock_an, mock_va, mock_dr, mock_cr):
                self.assertIs(cls_mock.call_args.kwargs["client"], mock_client)
            self.assertIs(worker_main.embedder._client, mock_client)

            worker_main.close_agents()
            mock_client.close.assert_called_once()
            self.assertIsNone(worker_main.genai_client)
            self.assertIsNone(worker_main.embedder._client)

    def test_process_vacancy_skip_and_proceed_with_critic_flags(self):
        mock_an = MagicMock()
        mock_va = MagicMock()
        mock_dr = MagicMock()
        mock_cr = MagicMock()

        # SKIP branch
        mock_an.analyze.return_value = SimpleNamespace(
            match_score=30,
            decision="SKIP",
            extracted_stack=["PHP"],
            summary="Wrong stack",
            pros=[],
        )
        pub = MagicMock()
        with patch.object(worker_main, "analyzer", mock_an), \
             patch.object(worker_main, "validator", mock_va), \
             patch.object(worker_main, "drafter", mock_dr), \
             patch.object(worker_main, "critic", mock_cr), \
             patch("main.get_recent_feedback_reasons", return_value=[]):
            worker_main.process_vacancy({"Title": "PHP Dev", "Link": "https://x", "Description": "PHP"}, pub)
        pub.assert_not_called()
        mock_va.validate.assert_not_called()

        # PROCEED branch with fallback_used=True -> exact 7 keys + warning in summary
        mock_an.analyze.return_value = SimpleNamespace(
            match_score=85,
            decision="PROCEED",
            extracted_stack=["Go"],
            summary="Great Go role.",
            pros=["Go"],
        )
        mock_va.validate.return_value = SimpleNamespace(verified_skills=[], unverified_skills=[])
        mock_dr.draft.return_value = SimpleNamespace(word_count=50, cover_letter="Draft text")
        mock_cr.review.return_value = SimpleNamespace(
            critique_score=0,
            fluff_percentage=0,
            final_letter="Draft text",
            fallback_used=True,
            is_approved=False,
            hallucinations_detected=[],
        )

        with patch.object(worker_main, "analyzer", mock_an), \
             patch.object(worker_main, "validator", mock_va), \
             patch.object(worker_main, "drafter", mock_dr), \
             patch.object(worker_main, "critic", mock_cr), \
             patch("main.get_recent_feedback_reasons", return_value=["Too senior"]):
            worker_main.process_vacancy(
                {"Title": "<b>Go</b> Dev", "Link": "https://x", "Description": "<p>Go</p>"},
                pub,
            )

        pub.assert_called_once()
        payload = pub.call_args[0][0]
        self.assertEqual(
            set(payload.keys()),
            {"title", "link", "match_score", "summary", "pros", "fluff_percentage", "final_letter"},
        )
        self.assertEqual(payload["title"], "Go Dev")
        self.assertTrue(payload["summary"].startswith("[⚠️ Critic audit skipped"))

    def test_main_daemon_joins_feedback_thread_and_closes_agents(self):
        mock_consumer = MagicMock()
        mock_thread = MagicMock()
        with patch.object(worker_main, "ensure_embeddings_exist"), \
             patch.object(worker_main, "init_agents"), \
             patch.object(worker_main, "close_agents") as mock_close_agents, \
             patch.object(worker_main, "start_feedback_listener_thread", return_value=mock_thread) as mock_fb, \
             patch.object(worker_main, "RabbitMQConsumer", return_value=mock_consumer), \
             patch.object(worker_main.settings, "run_once", False):
            worker_main.main()

        mock_fb.assert_called_once()
        mock_consumer.connect.assert_called_once()
        mock_consumer.start_consuming.assert_called_once()
        mock_thread.join.assert_called_once_with(timeout=5.0)
        mock_consumer.close.assert_called_once()
        mock_close_agents.assert_called_once()


if __name__ == "__main__":
    unittest.main()
