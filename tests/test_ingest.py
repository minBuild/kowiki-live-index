import io
import json
import tarfile

import pytest

from kowiki.ingest import dump_entry, handle_change, iter_dump, run_backfill
from kowiki.wikimedia import RevisionGone, parse_sse


class RecordingIngestor:
    def __init__(self):
        self.calls = []

    def upsert(self, page_id, rev_id, title, html, event_ts, source):
        self.calls.append(("upsert", page_id, rev_id, title, source))

    def delete(self, page_id, rev_id, title, event_ts, source):
        self.calls.append(("delete", page_id, rev_id, title, source))

    def flush(self):
        pass


class FakeWiki:
    def __init__(self, gone=False):
        self.gone = gone

    def revision_html(self, rev_id):
        if self.gone:
            raise RevisionGone(rev_id)
        return f"<p>{rev_id}</p>"


def change(kind="edit", namespace=0, redirect=False):
    return {
        "page_change_kind": kind,
        "dt": "2026-10-01T02:47:13Z",
        "wiki_id": "kowiki",
        "page": {
            "page_id": 42,
            "page_title": "테스트_문서",
            "namespace_id": namespace,
            "is_redirect": redirect,
        },
        "revision": {"rev_id": 1001},
    }


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (change("edit"), ("upsert", 42, 1001, "테스트 문서", "live")),
        (change("create"), ("upsert", 42, 1001, "테스트 문서", "live")),
        (change("delete"), ("delete", 42, 1001, "테스트 문서", "live")),
        (change("edit", redirect=True), ("delete", 42, 1001, "테스트 문서", "live")),
    ],
)
def test_page_changes_become_doc_events(event, expected):
    ingestor = RecordingIngestor()

    handle_change(ingestor, FakeWiki(), event)

    assert ingestor.calls == [expected]


@pytest.mark.parametrize("event", [change("edit", namespace=1), change("visibility_change")])
def test_irrelevant_changes_are_ignored(event):
    ingestor = RecordingIngestor()

    assert handle_change(ingestor, FakeWiki(), event) is None
    assert ingestor.calls == []


def test_gone_revision_is_skipped():
    ingestor = RecordingIngestor()

    assert handle_change(ingestor, FakeWiki(gone=True), change("edit")) is None
    assert ingestor.calls == []


def dump_record(page_id, namespace=0):
    return {
        "name": f"문서 {page_id}",
        "identifier": page_id,
        "date_modified": "2025-02-27T10:00:00Z",
        "version": {"identifier": page_id * 10},
        "namespace": {"identifier": namespace},
        "article_body": {"html": "<p>본문</p>", "wikitext": "본문"},
    }


def make_dump(records) -> io.BytesIO:
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("kowiki_namespace_0_0.ndjson")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    buf.seek(0)
    return buf


def test_backfill_streams_dump_records():
    dump = make_dump([dump_record(1), dump_record(2, namespace=10), dump_record(3)])
    ingestor = RecordingIngestor()

    sent = run_backfill(ingestor, iter_dump(dump), workers=2)

    assert sent == 2
    assert sorted(ingestor.calls) == [
        ("upsert", 1, 10, "문서 1", "backfill"),
        ("upsert", 3, 30, "문서 3", "backfill"),
    ]


def test_backfill_limit_and_skip():
    dump = make_dump([dump_record(i) for i in range(1, 6)])
    ingestor = RecordingIngestor()

    run_backfill(ingestor, iter_dump(dump), limit=2, skip=1, workers=1)

    assert sorted(c[1] for c in ingestor.calls) == [2, 3]


def test_dump_entry_skips_records_without_html():
    record = dump_record(1)
    record["article_body"] = {}

    assert dump_entry(record) is None


def test_parse_sse_keeps_last_event_id():
    lines = [
        ":ok",
        "",
        "event: message",
        'id: [{"offset":1}]',
        'data: {"a":',
        "data: 1}",
        "",
        'data: {"b":2}',
        "",
    ]

    assert list(parse_sse(lines)) == [('[{"offset":1}]', '{"a":\n1}'), ('[{"offset":1}]', '{"b":2}')]
