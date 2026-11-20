from typing import List, Optional
from google import genai
from google.genai import types

from config.config import settings
from core.llm import call_with_retry


class Embedder:
    def __init__(self, api_key: Optional[str] = None, model_name: Optional[str] = None):
        self._api_key = api_key
        self._client: Optional[genai.Client] = None
        self.model_name = model_name or settings.gemini_embedding_model
        self.dimension = 768

    def _get_client(self) -> genai.Client:
        if self._client is None:
            key = self._api_key if self._api_key is not None else settings.gemini_api_key
            if not key:
                raise RuntimeError("GEMINI_API_KEY is not configured for Embedder")
            self._client = genai.Client(api_key=key)
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
