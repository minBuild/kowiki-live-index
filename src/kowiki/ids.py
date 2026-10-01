import hashlib
import uuid
from collections import Counter

NAMESPACE = uuid.UUID("6f1c2b4e-8d3a-4c57-9b0e-2a7d5e9f1c3b")


def doc_id(wiki: str, page_id: int) -> str:
    return f"{wiki}:{page_id}"


def point_id(name: str) -> str:
    return str(uuid.uuid5(NAMESPACE, name))


def chunk_ids(doc: str, inputs: list[str]) -> list[str]:
    """임베딩 입력 내용으로 청크 ID를 만든다."""
    seen: Counter[str] = Counter()
    ids = []
    for text in inputs:
        digest = hashlib.sha256(text.encode()).hexdigest()
        ids.append(point_id(f"{doc}:{digest}:{seen[digest]}"))
        seen[digest] += 1
    return ids
