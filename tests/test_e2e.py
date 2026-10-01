import threading
import time
import uuid

import pytest
from prometheus_client import REGISTRY

from kowiki.blobstore import FileBlobStore
from kowiki.embedder import HashEmbedder
from kowiki.index import QdrantIndex
from kowiki.ingest import Ingestor
from kowiki.kafka import ensure_topics, make_producer
from kowiki.settings import Settings
from kowiki.stages import build_stage

pytestmark = pytest.mark.e2e


def wait_until(condition, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError("condition not met in time")


def stale_count() -> float:
    return REGISTRY.get_sample_value("kowiki_index_ops_total", {"result": "stale"}) or 0


def point_ids(index: QdrantIndex, doc_id: str) -> set[str]:
    points, _ = index.client.scroll(
        index.collection,
        scroll_filter={"must": [{"key": "doc_id", "match": {"value": doc_id}}]},
        limit=1000,
    )
    return {str(p.id) for p in points}


@pytest.fixture
def pipeline(tmp_path):
    run = uuid.uuid4().hex[:8]
    settings = Settings(
        topic_prefix=f"e2e-{run}.",
        group_prefix=f"e2e-{run}",
        collection=f"e2e_{run}",
        embed_backend="hash",
        embed_dim=64,
        batch_size=8,
        claim_check_bytes=20_000,
    )
    ensure_topics(settings)
    blobs = FileBlobStore(tmp_path)
    index = QdrantIndex.from_settings(settings)
    index.bootstrap(64)
    stages = [
        build_stage(name, settings, blobs=blobs, embedder=HashEmbedder(64), index=index)
        for name in ("parse", "chunk", "embed", "index")
    ]
    threads = [threading.Thread(target=stage.run, daemon=True) for stage in stages]
    for thread in threads:
        thread.start()
    yield Ingestor(settings, blobs, make_producer(settings)), index
    for stage in stages:
        stage.stop()
    for thread in threads:
        thread.join(timeout=30)
    index.client.delete_collection(index.collection)
    index.client.delete_collection(index.registry)


def test_update_stale_and_delete(pipeline, article_html):
    ingestor, index = pipeline

    ingestor.upsert(1, 100, "테스트 도시", article_html, 1_000, "test")
    ingestor.flush()
    wait_until(lambda: index.version_of("kowiki:1") == (100, 1_000))
    first = point_ids(index, "kowiki:1")
    assert len(first) >= 4

    edited = article_html.replace("근대에는 철도가 놓였다.", "근대에는 전차가 다녔다.")
    ingestor.upsert(1, 101, "테스트 도시", edited, 2_000, "test")
    ingestor.flush()
    wait_until(lambda: index.version_of("kowiki:1") == (101, 2_000))
    second = point_ids(index, "kowiki:1")
    assert len(first & second) == len(first) - 1

    before = stale_count()
    ingestor.upsert(1, 100, "테스트 도시", article_html, 3_000, "backfill")
    ingestor.flush()
    wait_until(lambda: stale_count() == before + 1)
    assert index.version_of("kowiki:1") == (101, 2_000)
    assert point_ids(index, "kowiki:1") == second

    ingestor.delete(1, 101, "테스트 도시", 4_000, "test")
    ingestor.flush()
    wait_until(lambda: index.count("kowiki:1") == 0)
    assert index.version_of("kowiki:1") == (101, 4_000)


def test_large_document_goes_through_claim_check(pipeline):
    ingestor, index = pipeline
    paragraphs = "".join(f"<p>{i}번째 문단은 큰 문서를 만들기 위한 문장이다.</p>" for i in range(1500))
    html = f'<html><body><section data-mw-section-id="0">{paragraphs}</section></body></html>'

    ingestor.upsert(2, 200, "큰 문서", html, 1_000, "test")
    ingestor.flush()

    wait_until(lambda: index.version_of("kowiki:2") == (200, 1_000))
    assert index.count("kowiki:2") > 50
