from fastapi.testclient import TestClient

from kowiki.embedder import HashEmbedder
from kowiki.search import create_app
from kowiki.settings import Settings
from kowiki.stages import EmbedStep, IndexStep
from tests.helpers import chunked_doc


def test_search_returns_matching_chunk(index):
    embedder = HashEmbedder(16)
    IndexStep(index)(EmbedStep(embedder, index)([chunked_doc(1, ["조선의 과거 제도", "부산 지하철 노선"])]))
    client = TestClient(create_app(Settings(), embedder, index))

    resp = client.get("/search", params={"q": "과거 제도", "k": 1})

    assert resp.status_code == 200
    [hit] = resp.json()["hits"]
    assert hit["text"] == "조선의 과거 제도"
    assert hit["doc_id"] == "kowiki:1"


def test_empty_query_is_rejected(index):
    client = TestClient(create_app(Settings(), HashEmbedder(16), index))

    assert client.get("/search", params={"q": ""}).status_code == 422
