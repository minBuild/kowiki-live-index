import math
import zlib
from typing import Protocol

import httpx

from kowiki.errors import TransientError
from kowiki.settings import Settings


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class OpenAIEmbedder:
    """OpenAI 호환 /embeddings 클라이언트. TEI, Ollama, OpenAI가 같은 형식을 쓴다."""

    def __init__(
        self,
        base_url: str,
        model: str,
        batch_size: int = 32,
        api_key: str | None = None,
        timeout: float = 120,
    ):
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.http = httpx.Client(base_url=base_url.rstrip("/"), headers=headers, timeout=timeout)
        self.model = model
        self.batch_size = batch_size

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors = []
        for i in range(0, len(texts), self.batch_size):
            vectors += self._request(texts[i : i + self.batch_size])
        return vectors

    def _request(self, batch: list[str]) -> list[list[float]]:
        try:
            resp = self.http.post("/embeddings", json={"model": self.model, "input": batch})
        except httpx.TransportError as e:
            raise TransientError(f"embedding server unreachable: {e}") from e
        if resp.status_code == 429 or resp.status_code >= 500:
            raise TransientError(f"embedding server returned {resp.status_code}")
        resp.raise_for_status()
        data = sorted(resp.json()["data"], key=lambda d: d["index"])
        return [d["embedding"] for d in data]


class HashEmbedder:
    """글자 n-gram을 해싱한 가짜 임베딩. 모델 없이 파이프라인만 돌려볼 때 쓴다."""

    def __init__(self, dim: int = 256):
        self.dim = dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(text) for text in texts]

    def _vector(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        chars = "".join(text.split())
        grams = list(chars) + [a + b for a, b in zip(chars, chars[1:], strict=False)]
        for gram in grams:
            h = zlib.crc32(gram.encode())
            vec[h % self.dim] += 1.0 if h & 0x80000000 else -1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


def make_embedder(settings: Settings) -> Embedder:
    if settings.embed_backend == "hash":
        return HashEmbedder(settings.embed_dim or 256)
    return OpenAIEmbedder(
        settings.embed_base_url, settings.embed_model, settings.embed_batch, settings.embed_api_key
    )
