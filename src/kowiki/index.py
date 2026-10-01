import functools
import logging

import httpx
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from kowiki.errors import TransientError
from kowiki.ids import point_id
from kowiki.models import EmbeddedChunk, EmbeddedDoc, Op
from kowiki.settings import Settings

log = logging.getLogger(__name__)


def _transient(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except (ResponseHandlingException, httpx.TransportError) as e:
            raise TransientError(f"qdrant: {e}") from e
        except UnexpectedResponse as e:
            if e.status_code in (404, 429) or e.status_code >= 500:
                raise TransientError(f"qdrant {e.status_code}: {e.reason_phrase}") from e
            raise

    return wrapper


class QdrantIndex:
    """청크 컬렉션과 문서별 버전 레지스트리를 관리한다."""

    def __init__(self, client: QdrantClient, alias: str, version: str):
        self.client = client
        self.alias = alias
        self.collection = f"{alias}_{version}"
        self.registry = f"{alias}_{version}_docs"

    @classmethod
    def from_settings(cls, settings: Settings, version: str | None = None) -> "QdrantIndex":
        client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key, timeout=30)
        return cls(client, settings.collection, version or settings.collection_version)

    def bootstrap(self, dim: int) -> None:
        if not self.client.collection_exists(self.collection):
            self.client.create_collection(
                self.collection,
                vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
            )
            self.client.create_payload_index(self.collection, "doc_id", models.PayloadSchemaType.KEYWORD)
            log.info("created collection %s (dim=%d)", self.collection, dim)
        if not self.client.collection_exists(self.registry):
            self.client.create_collection(
                self.registry, vectors_config=models.VectorParams(size=1, distance=models.Distance.DOT)
            )
        if self.alias not in self._aliases():
            self.switch_alias()

    def switch_alias(self) -> None:
        ops: list = []
        if self.alias in self._aliases():
            ops.append(models.DeleteAliasOperation(delete_alias=models.DeleteAlias(alias_name=self.alias)))
        ops.append(
            models.CreateAliasOperation(
                create_alias=models.CreateAlias(collection_name=self.collection, alias_name=self.alias)
            )
        )
        self.client.update_collection_aliases(change_aliases_operations=ops)
        log.info("alias %s -> %s", self.alias, self.collection)

    def _aliases(self) -> dict[str, str]:
        return {a.alias_name: a.collection_name for a in self.client.get_aliases().aliases}

    @_transient
    def version_of(self, doc_id: str) -> tuple[int, int] | None:
        points = self.client.retrieve(self.registry, [point_id(doc_id)], with_payload=True)
        if not points:
            return None
        payload = points[0].payload or {}
        return payload["rev_id"], payload["event_ts"]

    @_transient
    def vectors(self, ids: list[str]) -> dict[str, list[float]]:
        if not ids:
            return {}
        points = self.client.retrieve(self.collection, ids, with_payload=False, with_vectors=True)
        return {str(p.id): p.vector for p in points}

    @_transient
    def apply(self, doc: EmbeddedDoc) -> str:
        """문서 한 건을 반영한다. 같거나 더 새 버전이 이미 반영돼 있으면 건너뛴다."""
        current = self.version_of(doc.doc_id)
        if current is not None and doc.version <= current:
            return "stale"
        if doc.op is Op.DELETE or not doc.chunks:
            selector = models.FilterSelector(filter=models.Filter(must=[_doc_match(doc.doc_id)]))
            self.client.delete(self.collection, points_selector=selector, wait=True)
            result = "delete"
        else:
            self._replace(doc)
            result = "upsert"
        state = {
            "doc_id": doc.doc_id,
            "rev_id": doc.rev_id,
            "event_ts": doc.event_ts,
            "deleted": result == "delete",
            "chunks": len(doc.chunks),
        }
        self.client.upsert(
            self.registry,
            points=[models.PointStruct(id=point_id(doc.doc_id), vector=[1.0], payload=state)],
            wait=True,
        )
        return result

    def _replace(self, doc: EmbeddedDoc) -> None:
        ids = [c.id for c in doc.chunks]
        present = {str(p.id) for p in self.client.retrieve(self.collection, ids, with_payload=False)}
        fresh = [
            models.PointStruct(id=c.id, vector=c.floats(), payload=_payload(doc, c))
            for c in doc.chunks
            if c.id not in present
        ]
        for i in range(0, len(fresh), 128):
            self.client.upsert(self.collection, points=fresh[i : i + 128], wait=True)
        if present:
            self.client.set_payload(
                self.collection,
                payload={"rev_id": doc.rev_id, "title": doc.title, "url": doc.url},
                points=list(present),
                wait=True,
            )
        stale = models.Filter(must=[_doc_match(doc.doc_id)], must_not=[models.HasIdCondition(has_id=ids)])
        self.client.delete(self.collection, points_selector=models.FilterSelector(filter=stale), wait=True)

    @_transient
    def search(self, vector: list[float], limit: int) -> list[models.ScoredPoint]:
        return self.client.query_points(self.alias, query=vector, limit=limit, with_payload=True).points

    @_transient
    def count(self, doc_id: str | None = None) -> int:
        flt = models.Filter(must=[_doc_match(doc_id)]) if doc_id else None
        return self.client.count(self.collection, count_filter=flt, exact=True).count


def _doc_match(doc_id: str) -> models.FieldCondition:
    return models.FieldCondition(key="doc_id", match=models.MatchValue(value=doc_id))


def _payload(doc: EmbeddedDoc, chunk: EmbeddedChunk) -> dict:
    return {
        "doc_id": doc.doc_id,
        "rev_id": doc.rev_id,
        "title": doc.title,
        "url": doc.url,
        "heading": chunk.heading,
        "text": chunk.text,
        "ordinal": chunk.ordinal,
    }
