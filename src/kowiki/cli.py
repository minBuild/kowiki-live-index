import argparse
import logging
import signal
import threading
import time
from collections.abc import Callable

import uvicorn

from kowiki import metrics
from kowiki.blobstore import open_blobstore
from kowiki.embedder import make_embedder
from kowiki.index import QdrantIndex
from kowiki.ingest import Checkpoint, Ingestor, iter_dump, open_source, run_backfill, run_live, run_seed
from kowiki.kafka import ensure_topics, header, make_consumer, make_producer, produce
from kowiki.search import create_app
from kowiki.settings import Settings, get_settings
from kowiki.stages import build_stage
from kowiki.topics import dlq
from kowiki.wikimedia import WikiClient

log = logging.getLogger("kowiki")

STAGES = ("parse", "chunk", "embed", "index")


def wait_for[T](name: str, fn: Callable[[], T], timeout: float = 300) -> T:
    deadline = time.monotonic() + timeout
    while True:
        try:
            return fn()
        except Exception as e:
            if time.monotonic() > deadline:
                raise
            log.info("waiting for %s: %s", name, e)
            time.sleep(3)


def on_signal(handler: Callable[[], None]) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: handler())


def cmd_bootstrap(settings: Settings, args: argparse.Namespace) -> None:
    if not args.no_topics:
        wait_for("kafka", lambda: ensure_topics(settings))
    blobs = open_blobstore(settings)
    wait_for("blob store", blobs.ensure)
    dim = settings.embed_dim or wait_for(
        "embedding server", lambda: len(make_embedder(settings).embed(["차원 확인"])[0])
    )
    index = QdrantIndex.from_settings(settings)
    wait_for("qdrant", lambda: index.bootstrap(dim))
    log.info("bootstrap done (collection=%s, dim=%d)", index.collection, dim)


def cmd_stage(settings: Settings, args: argparse.Namespace) -> None:
    stage = build_stage(args.command, settings)
    on_signal(stage.stop)
    metrics.serve(settings.metrics_port)
    stage.run()


def cmd_live(settings: Settings, args: argparse.Namespace) -> None:
    blobs = open_blobstore(settings)
    ingestor = Ingestor(settings, blobs, make_producer(settings))
    stop = threading.Event()
    on_signal(stop.set)
    metrics.serve(settings.metrics_port)
    wiki = WikiClient(settings.wiki_domain, settings.user_agent)
    run_live(settings, ingestor, wiki, Checkpoint(blobs, f"checkpoints/live-{settings.wiki}"), stop.is_set)


def cmd_seed(settings: Settings, args: argparse.Namespace) -> None:
    ingestor = Ingestor(settings, open_blobstore(settings), make_producer(settings))
    sent = run_seed(ingestor, WikiClient(settings.wiki_domain, settings.user_agent), args.count, args.top)
    log.info("seeded %d pages", sent)


def cmd_backfill(settings: Settings, args: argparse.Namespace) -> None:
    ingestor = Ingestor(settings, open_blobstore(settings), make_producer(settings))
    records = iter_dump(open_source(args.source, settings.user_agent))
    sent = run_backfill(ingestor, records, args.limit, args.skip, args.workers)
    log.info("backfill sent %d docs", sent)


def cmd_search_api(settings: Settings, args: argparse.Namespace) -> None:
    uvicorn.run(create_app(settings), host=args.host, port=args.port)


def cmd_replay(settings: Settings, args: argparse.Namespace) -> None:
    consumer = make_consumer(settings, f"{settings.group_prefix}-replay")
    producer = make_producer(settings)
    consumer.subscribe([dlq(args.topic)])
    moved, idle_since = 0, time.monotonic()
    try:
        while args.limit is None or moved < args.limit:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if time.monotonic() - idle_since > 10:
                    break
                continue
            idle_since = time.monotonic()
            target = (header(msg, "x-source-topic") or args.topic.encode()).decode()
            reason = (header(msg, "x-error") or b"").decode()[:200]
            log.info("%s@%d -> %s: %s", msg.topic(), msg.offset(), target, reason)
            if not args.dry_run:
                headers = [h for h in msg.headers() or [] if not h[0].startswith("x-")]
                produce(producer, target, msg.key(), msg.value(), headers)
            moved += 1
        if moved and not args.dry_run:
            if producer.flush(30):
                raise RuntimeError("replay: some messages were not delivered")
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    log.info("%s %d messages", "would replay" if args.dry_run else "replayed", moved)


def cmd_switch_alias(settings: Settings, args: argparse.Namespace) -> None:
    QdrantIndex.from_settings(settings, args.version).switch_alias()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kowiki", description="한국어 위키백과 실시간 색인 파이프라인")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("bootstrap", help="토픽, 버킷, Qdrant 컬렉션을 만든다")
    p.add_argument("--no-topics", action="store_true", help="토픽을 Strimzi 등 외부에서 관리할 때")
    p.set_defaults(func=cmd_bootstrap)

    for name in STAGES:
        sub.add_parser(name, help=f"{name} 스테이지 워커").set_defaults(func=cmd_stage)

    sub.add_parser("live", help="실시간 편집 스트림을 수집한다").set_defaults(func=cmd_live)

    p = sub.add_parser("seed", help="API로 문서 일부만 수집한다")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--top", action="store_true", help="지난달 조회수 상위 문서")
    p.set_defaults(func=cmd_seed)

    p = sub.add_parser("backfill", help="Enterprise HTML 덤프를 적재한다")
    p.add_argument("source", help="덤프 URL 또는 로컬 경로")
    p.add_argument("--limit", type=int)
    p.add_argument("--skip", type=int, default=0, help="앞에서부터 건너뛸 레코드 수")
    p.add_argument("--workers", type=int, default=8)
    p.set_defaults(func=cmd_backfill)

    p = sub.add_parser("search-api", help="검색 API 서버")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=cmd_search_api)

    p = sub.add_parser("replay", help="DLQ에 쌓인 메시지를 원래 토픽으로 되돌린다")
    p.add_argument("topic", help="원래 토픽 (예: doc.events)")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_replay)

    p = sub.add_parser("switch-alias", help="검색 alias를 다른 버전 컬렉션으로 바꾼다")
    p.add_argument("version")
    p.set_defaults(func=cmd_switch_alias)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args.func(settings, args)
