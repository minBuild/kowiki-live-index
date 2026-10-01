from kowiki.embedder import HashEmbedder
from kowiki.ids import chunk_ids
from kowiki.models import EmbeddedChunk, EmbeddedDoc, Op, pack_vector

EMBEDDER = HashEmbedder(16)


def doc(rev: int, ts: int, texts: list[str], op: Op = Op.UPSERT) -> EmbeddedDoc:
    ids = chunk_ids("kowiki:1", texts)
    vectors = EMBEDDER.embed(texts)
    chunks = [
        EmbeddedChunk(id=i, ordinal=n, heading="문서", text=t, tokens=len(t), vector=pack_vector(v))
        for n, (i, t, v) in enumerate(zip(ids, texts, vectors, strict=True))
    ]
    return EmbeddedDoc(doc_id="kowiki:1", op=op, rev_id=rev, event_ts=ts, title="문서", chunks=chunks)


def stored_ids(index) -> set[str]:
    points, _ = index.client.scroll(index.collection, limit=100)
    return {str(p.id) for p in points}


def test_update_removes_chunks_that_disappeared(index):
    index.apply(doc(1, 100, ["가나다", "라마바", "사아자"]))
    index.apply(doc(2, 200, ["가나다", "차카타"]))

    assert stored_ids(index) == set(chunk_ids("kowiki:1", ["가나다", "차카타"]))
    assert index.version_of("kowiki:1") == (2, 200)


def test_older_revision_is_ignored(index):
    index.apply(doc(2, 200, ["새 내용"]))

    assert index.apply(doc(1, 300, ["옛 내용"])) == "stale"
    assert stored_ids(index) == set(chunk_ids("kowiki:1", ["새 내용"]))


def test_replaying_same_version_is_a_no_op(index):
    index.apply(doc(1, 100, ["가나다"]))

    assert index.apply(doc(1, 100, ["가나다"])) == "stale"


def test_late_event_does_not_resurrect_deleted_doc(index):
    index.apply(doc(5, 100, ["본문"]))
    index.apply(doc(5, 200, [], op=Op.DELETE))

    assert index.apply(doc(5, 150, ["본문"])) == "stale"
    assert stored_ids(index) == set()
    assert index.apply(doc(5, 300, ["되살린 본문"])) == "upsert"


def test_search_goes_through_alias(index):
    index.apply(doc(1, 100, ["서울의 역사", "부산의 교통"]))

    hits = index.search(EMBEDDER.embed(["서울 역사"])[0], limit=1)

    assert hits[0].payload["text"] == "서울의 역사"
