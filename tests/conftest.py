from pathlib import Path

import pytest
from qdrant_client import QdrantClient

from kowiki.index import QdrantIndex

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def article_html() -> str:
    return (FIXTURES / "article.html").read_text()


@pytest.fixture
def index() -> QdrantIndex:
    idx = QdrantIndex(QdrantClient(":memory:"), "test", "v1")
    idx.bootstrap(dim=16)
    return idx
