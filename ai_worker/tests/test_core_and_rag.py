import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
from google.genai import errors as genai_errors
from pgvector import Vector
from pydantic import BaseModel, ValidationError

AI_WORKER_DIR = Path(__file__).resolve().parent.parent
if str(AI_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(AI_WORKER_DIR))

from config.config import Config
from core.llm import call_with_retry, create_genai_client, is_transient_error
from rag import indexer
from rag.embedder import Embedder
from rag.retriever import VectorRetriever, _SIMILARITY_SQL


class TestCoreLLMAndConfig(unittest.TestCase):
    def test_create_genai_client_sets_30s_timeout(self):
        with patch("core.llm.genai.Client") as mock_ctor:
            create_genai_client("test-key")
        mock_ctor.assert_called_once()
        http_opts = mock_ctor.call_args.kwargs["http_options"]
        self.assertEqual(http_opts.timeout, 30_000)

    def test_is_transient_error_classification(self):
        self.assertTrue(is_transient_error(httpx.ReadTimeout("")))
        self.assertTrue(is_transient_error(httpx.ConnectError("")))
        self.assertTrue(is_transient_error(TimeoutError()))
        self.assertTrue(is_transient_error(genai_errors.APIError(429, {})))
        self.assertTrue(is_transient_error(genai_errors.APIError(503, {})))
        self.assertTrue(is_transient_error(RuntimeError("503 Service Unavailable")))

        self.assertFalse(
            is_transient_error(genai_errors.APIError(400, {"error": {"message": "connection invalid"}}))
        )
        self.assertFalse(is_transient_error(ValueError("error at line 500")))
        self.assertFalse(is_transient_error(TypeError("timeout arg wrong type")))

        class _M(BaseModel):
            x: int

        try:
            _M(x="not-an-int-500")
        except ValidationError as ve:
            self.assertFalse(is_transient_error(ve))

    def test_call_with_retry_exhaustion_reraises(self):
        fn = MagicMock(side_effect=httpx.ReadTimeout(""))
        with patch("core.llm.time.sleep"):
            with self.assertRaises(httpx.ReadTimeout):
                call_with_retry(fn, operation="op", max_attempts=3, base_delay=0.01)
        self.assertEqual(fn.call_count, 3)

    def test_load_profile_broken_json_and_non_dict_fail_fast(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_path = Path(tmpdir) / "candidate_profile.json"
            profile_path.write_text("{broken json", encoding="utf-8")

            cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with patch("config.config.PROJECT_ROOT", Path(tmpdir)), \
                     patch("config.config.CURRENT_DIR", Path(tmpdir) / "config"):
                    with self.assertRaises(ValueError):
                        Config()

                    profile_path.write_text('["not", "an", "object"]', encoding="utf-8")
                    with self.assertRaises(ValueError):
                        Config()

                    profile_path.unlink()
                    cfg = Config()
                    self.assertEqual(cfg.profile_data, {})
            finally:
                os.chdir(cwd)


class TestEmbedderAndRetrieverAndIndexer(unittest.TestCase):
    def test_embedder_client_lifecycle_and_batching(self):
        emb = Embedder(api_key="")
        with self.assertRaises(RuntimeError):
            emb.embed_text("hello")

        with self.assertRaises(ValueError):
            emb.embed_text("   ")

        with self.assertRaises(ValueError):
            emb.embed_texts(["ok", ""])

        shared = MagicMock()
        emb2 = Embedder(api_key="k")
        emb2.set_client(shared)
        shared.models.embed_content.return_value = SimpleNamespace(
            embeddings=[
                SimpleNamespace(values=[0.1] * 768),
                SimpleNamespace(values=[0.2] * 768),
            ]
        )
        vecs = emb2.embed_texts(["Go", "Postgres"])
        self.assertEqual(len(vecs), 2)
        shared.models.embed_content.assert_called_once()
        emb2.close()
        # Shared client must NOT be closed by Embedder.close()
        shared.close.assert_not_called()

        # Owned client MUST be closed
        owned = MagicMock()
        with patch("rag.embedder.create_genai_client", return_value=owned):
            emb3 = Embedder(api_key="k")
            owned.models.embed_content.return_value = SimpleNamespace(
                embeddings=[SimpleNamespace(values=[0.3] * 768)]
            )
            emb3.embed_text("Go")
            emb3.close()
        owned.close.assert_called_once()

    def test_retriever_hnsw_sql_vector_param_threshold_and_dedup(self):
        self.assertIn("ORDER BY embedding <=> %s ASC", _SIMILARITY_SQL)
        self.assertIn("LIMIT %s", _SIMILARITY_SQL)

        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            ("SplitCore", "DB", "pgxpool", 0.81234),
            ("Other", "Old", "unused", 0.30),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        retriever = VectorRetriever()
        with patch("rag.retriever.embedder.embed_texts", return_value=[[0.1] * 768]) as mock_embed_batch, \
             patch("rag.retriever.psycopg2.connect", return_value=mock_conn) as mock_pg, \
             patch("rag.retriever.register_vector") as mock_reg:
            res = retriever.search_evidence_batch(["Go", "go", "  Go ", ""], limit=2, threshold=0.5)

        mock_embed_batch.assert_called_once_with(["Go"])
        mock_reg.assert_called_once_with(mock_conn)
        self.assertEqual(mock_pg.call_args.kwargs["connect_timeout"], 10)
        self.assertIn("statement_timeout=", mock_pg.call_args.kwargs["options"])

        sql, params = mock_cur.execute.call_args[0]
        self.assertIsInstance(params[0], Vector)
        self.assertIsInstance(params[1], Vector)
        self.assertEqual(params[2], 2)

        self.assertEqual(len(res["Go"]), 1)
        self.assertEqual(res["Go"][0]["similarity"], 0.812)
        self.assertEqual(len(res["go"]), 1)
        self.assertEqual(res[""], [])
        mock_conn.close.assert_called_once()

    def test_retriever_embeds_before_opening_pg_connection(self):
        retriever = VectorRetriever()
        with patch("rag.retriever.embedder.embed_text", side_effect=RuntimeError("gemini down")), \
             patch("rag.retriever.psycopg2.connect") as mock_pg:
            with self.assertRaises(RuntimeError):
                retriever.search_evidence("Go")
        mock_pg.assert_not_called()

    def test_indexer_validates_chunks_upfront_and_wraps_vector(self):
        bad_cases = [
            "not-a-list",
            [{"project": "P", "category": "C"}],  # missing content
            [{"project": "P" * 101, "category": "C", "content": "ok"}],
            [{"project": "P", "category": "C" * 51, "content": "ok"}],
        ]
        for bad in bad_cases:
            with patch.object(indexer.settings, "profile_data", {"knowledge_chunks": bad}), \
                 patch.object(indexer.embedder, "embed_text") as mock_emb, \
                 patch("rag.indexer.psycopg2.connect") as mock_pg:
                with self.assertRaises(ValueError):
                    indexer.index_knowledge_base()
                mock_emb.assert_not_called()
                mock_pg.assert_not_called()

        # Happy path
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        good_chunks = [{"project": "SplitCore", "category": "DB", "content": "pgxpool"}]
        with patch.object(indexer.settings, "profile_data", {"knowledge_chunks": good_chunks}), \
             patch.object(indexer.embedder, "embed_text", return_value=[0.5] * 768), \
             patch("rag.indexer.psycopg2.connect", return_value=mock_conn), \
             patch("rag.indexer.register_vector"):
            indexer.index_knowledge_base()

        insert_call = mock_cur.execute.call_args_list[-1]
        self.assertIsInstance(insert_call[0][1][3], Vector)
        mock_conn.close.assert_called_once()

    def test_importing_indexer_does_not_create_logs_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            proc = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    f"import sys; sys.path.insert(0, {str(AI_WORKER_DIR)!r}); import rag.indexer",
                ],
                cwd=tmpdir,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertFalse((Path(tmpdir) / "logs").exists())


if __name__ == "__main__":
    unittest.main()
