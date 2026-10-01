import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

MESSAGES = Counter("kowiki_messages_total", "Messages handled by a stage", ["stage", "result"])
BATCH_SECONDS = Histogram(
    "kowiki_batch_seconds",
    "Time spent on one consumed batch",
    ["stage"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
)
RETRIES = Counter("kowiki_transient_retries_total", "Retries after transient errors", ["stage"])
CHUNKS = Counter("kowiki_chunks_total", "Chunks seen by the embed stage", ["result"])
INDEX_OPS = Counter("kowiki_index_ops_total", "Index results per document", ["result"])
INGESTED = Counter("kowiki_ingested_total", "Events written to doc.events", ["source", "op"])
FRESHNESS = Histogram(
    "kowiki_freshness_seconds",
    "Delay from the source change to the index update",
    ["source"],
    buckets=(1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1800, 3600),
)
SEARCH_SECONDS = Histogram("kowiki_search_seconds", "Search request latency")

_last_beat = time.monotonic()


def beat() -> None:
    global _last_beat
    _last_beat = time.monotonic()


def serve(port: int, max_silence: float = 300) -> ThreadingHTTPServer:
    """/metrics와 /healthz를 백그라운드 스레드에서 띄운다."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                self._reply(200, generate_latest(), CONTENT_TYPE_LATEST)
            elif self.path == "/healthz":
                alive = time.monotonic() - _last_beat < max_silence
                self._reply(200 if alive else 503, b"ok" if alive else b"stalled", "text/plain")
            else:
                self._reply(404, b"not found", "text/plain")

        def _reply(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
