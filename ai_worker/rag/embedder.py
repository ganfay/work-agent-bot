from typing import List, Optional
from google import genai
from google.genai import types

from config.config import settings
from core.llm import DEFAULT_GEMINI_TIMEOUT_MS, call_with_retry, create_genai_client


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
        client = self._get_client()

        def _call():
            response = client.models.embed_content(
                model=self.model_name,
                contents=text,
                config=types.EmbedContentConfig(output_dimensionality=self.dimension),
            )
            if not response.embeddings:
                raise RuntimeError("Gemini embed_content returned empty embeddings")
            return list(response.embeddings[0].values)

        return call_with_retry(_call, operation="Embedder.embed_text")


embedder = Embedder()
