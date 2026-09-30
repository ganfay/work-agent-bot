from typing import List
from google import genai
from google.genai import types

from config.config import settings


class Embedder:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = "gemini-embedding-001"
        self.dimension = 768

    def embed_text(self, text: str) -> List[float]:
        response = self.client.models.embed_content(
            model=self.model_name,
            contents=text,
            config=types.EmbedContentConfig(output_dimensionality=self.dimension),
        )
        return response.embeddings[0].values


embedder = Embedder(api_key=settings.gemini_api_key)
