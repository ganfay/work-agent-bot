from typing import List, Optional
from google import genai
from google.genai import types

from config.config import settings
from core.llm import DEFAULT_GEMINI_TIMEOUT_MS, call_with_retry, create_genai_client

_MAX_EMBED_BATCH_SIZE = 100


class Embedder:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        client: Optional[genai.Client] = None,
        timeout_ms: int = DEFAULT_GEMINI_TIMEOUT_MS,
    ):
        self._api_key = api_key
        self._client: Optional[genai.Client] = client
        self._owns_client = client is None
        self._timeout_ms = timeout_ms
        self.model_name = model_name or settings.gemini_embedding_model
        self.dimension = 768

    def set_client(self, client: Optional[genai.Client]) -> None:
        if self._client is not None and self._owns_client and self._client is not client:
            try:
                self._client.close()
            except Exception:
                pass
        self._client = client
        self._owns_client = client is None

    def close(self) -> None:
        if self._client is not None and self._owns_client:
            try:
                self._client.close()
            except Exception:
                pass
            finally:
                self._client = None
        elif not self._owns_client:
            self._client = None

    def _get_client(self) -> genai.Client:
        if self._client is None:
            key = self._api_key if self._api_key is not None else settings.gemini_api_key
            if not key:
                raise RuntimeError("GEMINI_API_KEY is not configured for Embedder")
            self._client = create_genai_client(key, timeout_ms=self._timeout_ms)
            self._owns_client = True
        return self._client

    def embed_text(self, text: str) -> List[float]:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Cannot embed empty text")
        client = self._get_client()

        def _call():
            response = client.models.embed_content(
                model=self.model_name,
                contents=text,
                config=types.EmbedContentConfig(output_dimensionality=self.dimension),
            )
            if not response.embeddings or not response.embeddings[0].values:
                raise RuntimeError("Gemini embed_content returned empty embeddings")
            return list(response.embeddings[0].values)

        return call_with_retry(_call, operation="Embedder.embed_text")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        for idx, item in enumerate(texts):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"Cannot embed empty text at index {idx}")

        if len(texts) == 1:
            return [self.embed_text(texts[0])]

        client = self._get_client()
        all_vectors: List[List[float]] = []

        for start in range(0, len(texts), _MAX_EMBED_BATCH_SIZE):
            batch = texts[start : start + _MAX_EMBED_BATCH_SIZE]

            def _call(batch_slice=batch):
                response = client.models.embed_content(
                    model=self.model_name,
                    contents=batch_slice,
                    config=types.EmbedContentConfig(output_dimensionality=self.dimension),
                )
                embeddings = response.embeddings or []
                if len(embeddings) != len(batch_slice):
                    raise RuntimeError(
                        f"Gemini embed_content returned {len(embeddings)} embeddings for {len(batch_slice)} inputs"
                    )
                vectors: List[List[float]] = []
                for emb in embeddings:
                    if not emb.values:
                        raise RuntimeError("Gemini embed_content returned empty embedding values")
                    vectors.append(list(emb.values))
                return vectors

            all_vectors.extend(call_with_retry(_call, operation="Embedder.embed_texts"))

        return all_vectors


embedder = Embedder()
