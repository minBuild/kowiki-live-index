from kowiki.chunker import SectionChunker
from kowiki.models import ChunkedDoc, Op, ParsedDoc, Section
from kowiki.stages import ChunkStep


class FakeMessage:
    def __init__(
        self, value: bytes, key: bytes = b"k", topic: str = "in", partition: int = 0, offset: int = 0
    ):
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._headers: list = []

    def value(self):
        return self._value

    def key(self):
        return self._key

    def headers(self):
        return self._headers

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        self.sent.append((topic, key, value, headers or []))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0


class FakeConsumer:
    def __init__(self):
        self.committed = []
        self.pauses = 0

    def commit(self, offsets=None, asynchronous=True):
        self.committed += offsets

    def assignment(self):
        return []

    def pause(self, partitions):
        self.pauses += 1

    def resume(self, partitions):
        pass

    def poll(self, timeout=None):
        return None

    def seek(self, partition):
        pass


def chunked_doc(rev: int, sections: list[str]) -> ChunkedDoc:
    parsed = ParsedDoc(
        doc_id="kowiki:1",
        op=Op.UPSERT,
        rev_id=rev,
        event_ts=rev,
        title="문서",
        sections=[Section(path=[s], blocks=[s]) for s in sections],
    )
    return ChunkStep(SectionChunker())([parsed])[0]
