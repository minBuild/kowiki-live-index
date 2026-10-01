import json
import logging
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta

import httpx

from kowiki.errors import TransientError

log = logging.getLogger(__name__)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1"


class RevisionGone(Exception):
    """삭제됐거나 숨겨져서 더 이상 읽을 수 없는 판."""


class WikiClient:
    """위키백과 REST·Action API와 EventStreams를 다룬다."""

    def __init__(self, domain: str, user_agent: str, timeout: float = 30):
        self.domain = domain
        self.headers = {"User-Agent": user_agent}
        self.http = httpx.Client(
            base_url=f"https://{domain}", headers=self.headers, timeout=timeout, follow_redirects=True
        )

    def revision_html(self, rev_id: int) -> str:
        resp = self._get(f"/w/rest.php/v1/revision/{rev_id}/html")
        if resp.status_code in (403, 404):
            raise RevisionGone(rev_id)
        resp.raise_for_status()
        return resp.text

    def random_pages(self, count: int) -> list[dict]:
        pages: dict[int, dict] = {}
        while len(pages) < count:
            batch = self._query(
                generator="random",
                grnnamespace=0,
                grnfilterredir="nonredirects",
                grnlimit=min(50, count - len(pages)),
            )
            pages.update({p["pageid"]: p for p in batch})
        return list(pages.values())[:count]

    def top_pages(self, count: int) -> list[dict]:
        month = datetime.now(UTC).replace(day=1) - timedelta(days=1)
        url = (
            "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
            f"{self.domain}/all-access/{month:%Y/%m}/all-days"
        )
        resp = self._get(url)
        resp.raise_for_status()
        articles = resp.json()["items"][0]["articles"]
        titles = [a["article"] for a in articles if ":" not in a["article"]][:count]
        pages = []
        for i in range(0, len(titles), 50):
            pages += self._query(titles="|".join(titles[i : i + 50]), redirects=1)
        return pages

    def page_changes(self, last_event_id: str | None = None) -> Iterator[tuple[str | None, dict]]:
        """page_change 스트림을 읽는다. 끊기면 마지막 이벤트 ID부터 다시 받는다."""
        delay = 1.0
        while True:
            headers = {**self.headers, "Accept": "text/event-stream"}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            try:
                with httpx.stream(
                    "GET", STREAM_URL, headers=headers, timeout=httpx.Timeout(30, read=120)
                ) as resp:
                    resp.raise_for_status()
                    delay = 1.0
                    for event_id, data in parse_sse(resp.iter_lines()):
                        last_event_id = event_id or last_event_id
                        try:
                            change = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                        yield last_event_id, change
            except httpx.HTTPError as e:
                log.warning("event stream disconnected: %s", e)
            time.sleep(delay)
            delay = min(delay * 2, 60)

    def _query(self, **params) -> list[dict]:
        params = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "prop": "revisions",
            "rvprop": "ids|timestamp",
            **params,
        }
        resp = self._get("/w/api.php", params=params)
        resp.raise_for_status()
        return [p for p in resp.json().get("query", {}).get("pages", []) if p.get("revisions")]

    def _get(self, url: str, **kwargs) -> httpx.Response:
        try:
            resp = self.http.get(url, **kwargs)
        except httpx.TransportError as e:
            raise TransientError(f"{self.domain}: {e}") from e
        if resp.status_code == 429 or resp.status_code >= 500:
            raise TransientError(f"{url} returned {resp.status_code}")
        return resp


def parse_sse(lines: Iterable[str]) -> Iterator[tuple[str | None, str]]:
    event_id, data = None, []
    for line in lines:
        if not line:
            if data:
                yield event_id, "\n".join(data)
                data = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value
