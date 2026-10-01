import time

from kowiki import metrics
from kowiki.blobstore import BlobStore, get_text, open_blobstore
from kowiki.chunker import FixedChunker, SectionChunker, make_chunker
from kowiki.embedder import Embedder, make_embedder
from kowiki.index import QdrantIndex
from kowiki.kafka import Codec, Stage
from kowiki.models import ChunkedDoc, DocEvent, EmbeddedChunk, EmbeddedDoc, Op, ParsedDoc, pack_vector
from kowiki.settings import Settings
from kowiki.topics import Topics
from kowiki.wiki_html import parse_article


class ParseStep:
    """원본 HTML을 읽어 섹션 구조로 바꾼다."""

    def __init__(self, blobs: BlobStore):
        self.blobs = blobs

    def __call__(self, events: list[DocEvent]) -> list[ParsedDoc]:
        return [self._parse(e) for e in events]

    def _parse(self, event: DocEvent) -> ParsedDoc:
        if event.op is Op.DELETE:
            return ParsedDoc(**event.ref())
        if not event.blob_uri:
            raise ValueError(f"{event.doc_id}: upsert without blob_uri")
        return ParsedDoc(**event.ref(), sections=parse_article(get_text(self.blobs, event.blob_uri)))


class ChunkStep:
    def __init__(self, chunker: SectionChunker | FixedChunker):
        self.chunker = chunker

    def __call__(self, docs: list[ParsedDoc]) -> list[ChunkedDoc]:
        return [
            ChunkedDoc(
                **d.ref(), chunks=self.chunker(d.doc_id, d.title, d.sections) if d.op is Op.UPSERT else []
            )
            for d in docs
        ]


class EmbedStep:
    """이미 색인된 청크는 벡터를 가져다 쓰고 새 청크만 임베딩한다."""

    def __init__(self, embedder: Embedder, index: QdrantIndex | None = None, doc_prefix: str = ""):
        self.embedder = embedder
        self.index = index
        self.doc_prefix = doc_prefix

    def __call__(self, docs: list[ChunkedDoc]) -> list[EmbeddedDoc]:
        chunks = [c for d in docs for c in d.chunks]
        vectors = self.index.vectors([c.id for c in chunks]) if self.index and chunks else {}
        todo = list({c.id: c for c in chunks if c.id not in vectors}.values())
        if todo:
            fresh = self.embedder.embed([self.doc_prefix + c.embed_input() for c in todo])
            vectors.update({c.id: v for c, v in zip(todo, fresh, strict=True)})
        metrics.CHUNKS.labels("embedded").inc(len(todo))
        metrics.CHUNKS.labels("reused").inc(len(chunks) - len(todo))
        return [
            EmbeddedDoc(
                **d.ref(),
                chunks=[EmbeddedChunk(**c.model_dump(), vector=pack_vector(vectors[c.id])) for c in d.chunks],
            )
            for d in docs
        ]


class IndexStep:
    """Qdrant에 반영하고 반영 지연을 기록한다."""

    def __init__(self, index: QdrantIndex):
        self.index = index

    def __call__(self, docs: list[EmbeddedDoc]) -> list[None]:
        for doc in docs:
            result = self.index.apply(doc)
            metrics.INDEX_OPS.labels(result).inc()
            if result != "stale":
                metrics.FRESHNESS.labels(doc.source).observe(max(0.0, time.time() - doc.event_ts / 1000))
        return [None] * len(docs)


def build_stage(
    name: str,
    settings: Settings,
    blobs: BlobStore | None = None,
    embedder: Embedder | None = None,
    index: QdrantIndex | None = None,
) -> Stage:
    topics = Topics(settings.topic_prefix)
    blobs = blobs or open_blobstore(settings)
    codec = Codec(blobs, settings.claim_check_bytes)
    if name == "parse":
        return Stage(name, settings, codec, topics.events, DocEvent, ParseStep(blobs), topics.parsed)
    if name == "chunk":
        step = ChunkStep(make_chunker(settings))
        return Stage(name, settings, codec, topics.parsed, ParsedDoc, step, topics.chunks)
    if name == "embed":
        if index is None and settings.reuse_vectors:
            index = QdrantIndex.from_settings(settings)
        step = EmbedStep(embedder or make_embedder(settings), index, settings.embed_doc_prefix)
        return Stage(name, settings, codec, topics.chunks, ChunkedDoc, step, topics.embeddings)
    if name == "index":
        step = IndexStep(index or QdrantIndex.from_settings(settings))
        return Stage(name, settings, codec, topics.embeddings, EmbeddedDoc, step)
    raise ValueError(f"unknown stage: {name}")
