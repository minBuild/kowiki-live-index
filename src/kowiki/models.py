import base64
import struct
from enum import StrEnum

from pydantic import BaseModel


class Op(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


class DocRef(BaseModel):
    """모든 단계의 메시지가 공유하는 문서 식별 정보."""

    doc_id: str
    op: Op
    rev_id: int
    event_ts: int
    title: str = ""
    url: str = ""
    source: str = ""

    @property
    def version(self) -> tuple[int, int]:
        return self.rev_id, self.event_ts

    def ref(self) -> dict:
        return self.model_dump(include=set(DocRef.model_fields))


class DocEvent(DocRef):
    blob_uri: str | None = None


class Section(BaseModel):
    path: list[str]
    blocks: list[str]


class ParsedDoc(DocRef):
    sections: list[Section] = []


class Chunk(BaseModel):
    id: str
    ordinal: int
    heading: str
    text: str
    tokens: int

    def embed_input(self) -> str:
        return f"{self.heading}\n{self.text}"


class ChunkedDoc(DocRef):
    chunks: list[Chunk] = []


class EmbeddedChunk(Chunk):
    vector: str

    def floats(self) -> list[float]:
        return unpack_vector(self.vector)


class EmbeddedDoc(DocRef):
    chunks: list[EmbeddedChunk] = []


def pack_vector(values: list[float]) -> str:
    return base64.b64encode(struct.pack(f"<{len(values)}f", *values)).decode()


def unpack_vector(data: str) -> list[float]:
    raw = base64.b64decode(data)
    return list(struct.unpack(f"<{len(raw) // 4}f", raw))
