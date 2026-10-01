import json
import logging
import tarfile
import time
import urllib.request
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import IO
from urllib.parse import quote

from confluent_kafka import Producer

from kowiki import metrics
from kowiki.blobstore import BlobStore, put_text
from kowiki.errors import TransientError, retry
from kowiki.ids import doc_id
from kowiki.kafka import produce
from kowiki.models import DocEvent, Op
from kowiki.settings import Settings
from kowiki.topics import Topics
from kowiki.wikimedia import RevisionGone, WikiClient

log = logging.getLogger(__name__)

UPSERT_KINDS = {"create", "edit", "move", "undelete"}


def to_millis(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp() * 1000)


class Ingestor:
    """원본 HTML을 오브젝트 스토리지에 올리고 doc.events에 이벤트를 쓴다."""

    def __init__(self, settings: Settings, blobs: BlobStore, producer: Producer):
        self.wiki = settings.wiki
        self.base_url = f"https://{settings.wiki_domain}/wiki/"
        self.topic = Topics(settings.topic_prefix).events
        self.blobs = blobs
        self.producer = producer
        self._errors: list = []

    def upsert(self, page_id: int, rev_id: int, title: str, html: str, event_ts: int, source: str) -> None:
        uri = put_text(self.blobs, f"raw/{self.wiki}/{page_id}/{rev_id}.html.gz", html)
        url = self.base_url + quote(title.replace(" ", "_"))
        self._emit(
            DocEvent(
                doc_id=doc_id(self.wiki, page_id),
                op=Op.UPSERT,
                rev_id=rev_id,
                event_ts=event_ts,
                title=title,
                url=url,
                source=source,
                blob_uri=uri,
            )
        )

    def delete(self, page_id: int, rev_id: int, title: str, event_ts: int, source: str) -> None:
        self._emit(
            DocEvent(
                doc_id=doc_id(self.wiki, page_id),
                op=Op.DELETE,
                rev_id=rev_id,
                event_ts=event_ts,
                title=title,
                source=source,
            )
        )

    def flush(self) -> None:
        pending = self.producer.flush(30)
        if pending or self._errors:
            raise RuntimeError(f"{pending} events undelivered, errors={self._errors[:3]}")

    def _emit(self, event: DocEvent) -> None:
        key = event.doc_id.encode()
        produce(self.producer, self.topic, key, event.model_dump_json().encode(), on_delivery=self._delivered)
        self.producer.poll(0)
        metrics.INGESTED.labels(event.source, event.op.value).inc()

    def _delivered(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(err)


class Checkpoint:
    """실시간 스트림에서 마지막으로 처리한 이벤트 ID를 보관한다."""

    def __init__(self, blobs: BlobStore, key: str):
        self.blobs = blobs
        self.key = key

    def load(self) -> str | None:
        data = self.blobs.read(self.key)
        return data.decode() if data else None

    def save(self, event_id: str) -> None:
        self.blobs.put(self.key, event_id.encode())


def handle_change(ingestor: Ingestor, wiki: WikiClient, change: dict) -> str | None:
    """page_change 이벤트를 doc.events로 옮긴다. 넘겨주기 전환은 삭제로 본다."""
    page = change["page"]
    kind = change["page_change_kind"]
    if page.get("namespace_id") != 0:
        return None
    rev_id = (change.get("revision") or {}).get("rev_id", 0)
    title = page["page_title"].replace("_", " ")
    ts = to_millis(change["dt"])
    if kind == "delete" or (kind in UPSERT_KINDS and page.get("is_redirect")):
        ingestor.delete(page["page_id"], rev_id, title, ts, "live")
        return "delete"
    if kind not in UPSERT_KINDS:
        return None
    try:
        html = retry(partial(wiki.revision_html, rev_id))
    except RevisionGone:
        log.info("revision %s of %s is gone", rev_id, title)
        return None
    ingestor.upsert(page["page_id"], rev_id, title, html, ts, "live")
    return "upsert"


def run_live(
    settings: Settings,
    ingestor: Ingestor,
    wiki: WikiClient,
    checkpoint: Checkpoint,
    should_stop: Callable[[], bool] = lambda: False,
) -> None:
    last_id = retry(checkpoint.load, attempts=8)
    log.info("live: starting from %s", "checkpoint" if last_id else "now")
    saved_at = time.monotonic()
    for event_id, change in wiki.page_changes(last_id):
        metrics.beat()
        if should_stop():
            break
        if change.get("wiki_id") == settings.wiki:
            try:
                result = handle_change(ingestor, wiki, change)
            except TransientError as e:
                log.error("live: gave up on %s: %s", change["page"].get("page_title"), e)
                result = None
            if result:
                log.info("live: %s %s", result, change["page"]["page_title"])
        if event_id:
            last_id = event_id
        if last_id and time.monotonic() - saved_at > 10:
            ingestor.flush()
            checkpoint.save(last_id)
            saved_at = time.monotonic()
    ingestor.flush()
    if last_id:
        checkpoint.save(last_id)


def run_seed(ingestor: Ingestor, wiki: WikiClient, count: int, top: bool = False) -> int:
    pages = wiki.top_pages(count) if top else wiki.random_pages(count)
    sent = 0
    for page in pages:
        rev = page["revisions"][0]
        try:
            html = retry(partial(wiki.revision_html, rev["revid"]))
        except RevisionGone:
            continue
        ingestor.upsert(
            page["pageid"], rev["revid"], page["title"], html, to_millis(rev["timestamp"]), "seed"
        )
        sent += 1
    ingestor.flush()
    return sent


def open_source(source: str, user_agent: str) -> IO[bytes]:
    if source.startswith(("http://", "https://")):
        request = urllib.request.Request(source, headers={"User-Agent": user_agent})
        return urllib.request.urlopen(request, timeout=60)
    return Path(source).open("rb")


def iter_dump(stream: IO[bytes]) -> Iterator[dict]:
    """Enterprise HTML 덤프(tar.gz 안의 NDJSON)를 스트리밍으로 읽는다."""
    with stream, tarfile.open(fileobj=stream, mode="r|gz") as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".ndjson"):
                continue
            for line in tar.extractfile(member):
                yield json.loads(line)


def dump_entry(record: dict) -> tuple[int, int, str, str, int] | None:
    if (record.get("namespace") or {}).get("identifier") != 0:
        return None
    html = (record.get("article_body") or {}).get("html")
    if not html:
        return None
    return (
        record["identifier"],
        record["version"]["identifier"],
        record["name"],
        html,
        to_millis(record["date_modified"]),
    )


def run_backfill(
    ingestor: Ingestor, records: Iterator[dict], limit: int | None = None, skip: int = 0, workers: int = 8
) -> int:
    sent = 0
    started = time.monotonic()
    pending: set[Future] = set()
    with ThreadPoolExecutor(workers) as pool:
        for n, record in enumerate(records):
            if n < skip:
                continue
            if limit is not None and sent >= limit:
                break
            entry = dump_entry(record)
            if entry is None:
                continue
            pending.add(pool.submit(ingestor.upsert, *entry, "backfill"))
            sent += 1
            if len(pending) >= workers * 4:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
            if sent % 1000 == 0:
                log.info(
                    "backfill: record %d, sent %d (%.0f docs/s)", n, sent, sent / (time.monotonic() - started)
                )
        for future in pending:
            future.result()
    ingestor.flush()
    return sent
