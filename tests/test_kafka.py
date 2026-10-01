import pytest

from kowiki.blobstore import FileBlobStore
from kowiki.errors import TransientError
from kowiki.kafka import CLAIM_HEADER, Codec, Stage
from kowiki.models import DocEvent, Op
from kowiki.settings import Settings
from tests.helpers import FakeConsumer, FakeMessage, FakeProducer


def event(n: int) -> DocEvent:
    return DocEvent(doc_id=f"kowiki:{n}", op=Op.DELETE, rev_id=n, event_ts=n, title=f"문서 {n}")


@pytest.fixture
def codec(tmp_path) -> Codec:
    return Codec(FileBlobStore(tmp_path), threshold=200)


def test_small_message_is_sent_inline(codec):
    value, headers = codec.encode("doc.events", event(1))

    assert headers == []
    assert b"kowiki:1" in value


def test_large_message_goes_through_claim_check(codec):
    big = event(2).model_copy(update={"title": "긴 제목" * 100})

    value, headers = codec.encode("doc.events", big)
    msg = FakeMessage(value)
    msg._headers = headers

    assert headers == [(CLAIM_HEADER, b"1")]
    assert len(value) < 200
    assert codec.decode(msg, DocEvent) == big


def make_stage(codec, handler):
    stage = Stage(
        "test",
        Settings(),
        codec,
        "in",
        DocEvent,
        handler,
        sink="out",
        consumer=FakeConsumer(),
        producer=FakeProducer(),
    )
    stage.retry_delay = 0.01
    return stage


def messages(codec, count):
    return [FakeMessage(codec.encode("in", event(i))[0], key=str(i).encode(), offset=i) for i in range(count)]


def test_batch_results_are_sent_and_offsets_committed(codec):
    stage = make_stage(codec, lambda items: items)

    stage.process(messages(codec, 3))

    assert [s[0] for s in stage.producer.sent] == ["out"] * 3
    assert [(tp.topic, tp.partition, tp.offset) for tp in stage.consumer.committed] == [("in", 0, 3)]


def test_failing_message_goes_to_dlq_and_others_continue(codec):
    def handler(items):
        if any(i.rev_id == 1 for i in items):
            raise ValueError("bad document")
        return items

    stage = make_stage(codec, handler)
    stage.process(messages(codec, 3))

    topics = [s[0] for s in stage.producer.sent]
    assert topics.count("out") == 2
    assert topics.count("in.dlq") == 1
    dead = next(s for s in stage.producer.sent if s[0] == "in.dlq")
    assert dead[1] == b"1"
    assert ("x-source-offset", b"1") in dead[3]
    assert stage.consumer.committed[-1].offset == 3


def test_transient_error_is_retried_without_dlq(codec):
    calls = []

    def handler(items):
        calls.append(len(items))
        if len(calls) < 3:
            raise TransientError("qdrant down")
        return items

    stage = make_stage(codec, handler)
    stage.process(messages(codec, 2))

    assert calls == [2, 2, 2]
    assert stage.consumer.pauses == 2
    assert [s[0] for s in stage.producer.sent] == ["out", "out"]


def test_undecodable_message_goes_to_dlq(codec):
    stage = make_stage(codec, lambda items: items)

    stage.process([FakeMessage(b"{not json", offset=7)])

    assert [s[0] for s in stage.producer.sent] == ["in.dlq"]
    assert stage.consumer.committed[-1].offset == 8
