import gzip
import json
import logging
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

from kowiki import metrics
from kowiki.blobstore import BlobStore
from kowiki.errors import TransientError
from kowiki.settings import Settings
from kowiki.topics import Topics, dlq

log = logging.getLogger(__name__)

CLAIM_HEADER = "claim-check"
DAY_MS = 24 * 60 * 60 * 1000


def make_producer(settings: Settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 20,
        }
    )


def make_consumer(settings: Settings, group: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "max.poll.interval.ms": settings.max_poll_interval_ms,
        }
    )


def ensure_topics(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = []
    for name in Topics(settings.topic_prefix).main():
        retention = {"retention.ms": str(7 * DAY_MS)}
        wanted.append(NewTopic(name, settings.topic_partitions, settings.topic_replicas, config=retention))
        wanted.append(
            NewTopic(dlq(name), 1, settings.topic_replicas, config={"retention.ms": str(30 * DAY_MS)})
        )
    missing = [t for t in wanted if t.topic not in existing]
    if not missing:
        return
    for name, future in admin.create_topics(missing).items():
        try:
            future.result()
            log.info("created topic %s", name)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def produce(producer: Producer, topic: str, key: bytes | None, value: bytes, headers=None, on_delivery=None):
    while True:
        try:
            producer.produce(topic, value=value, key=key, headers=headers, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(0.5)


def header(msg: Message, name: str) -> bytes | None:
    for key, value in msg.headers() or []:
        if key == name:
            return value
    return None


class Codec:
    """큰 메시지는 오브젝트 스토리지에 두고 참조만 보낸다(claim-check)."""

    def __init__(self, blobs: BlobStore, threshold: int):
        self.blobs = blobs
        self.threshold = threshold

    def encode(self, topic: str, model: BaseModel) -> tuple[bytes, list[tuple[str, bytes]]]:
        data = model.model_dump_json().encode()
        if len(data) <= self.threshold:
            return data, []
        uri = self.blobs.put(f"claims/{topic}/{uuid.uuid4().hex}.json.gz", gzip.compress(data))
        return json.dumps({"ref": uri}).encode(), [(CLAIM_HEADER, b"1")]

    def decode[M: BaseModel](self, msg: Message, model: type[M]) -> M:
        data = msg.value()
        if header(msg, CLAIM_HEADER):
            data = gzip.decompress(self.blobs.get(json.loads(data)["ref"]))
        return model.model_validate_json(data)


class Interrupted(Exception):
    pass


@dataclass
class Failed:
    error: Exception


class Stage:
    """입력 토픽을 배치로 읽어 처리하고, 결과를 다음 토픽에 쓴 뒤 오프셋을 커밋한다."""

    def __init__(
        self,
        name: str,
        settings: Settings,
        codec: Codec,
        source: str,
        model: type[BaseModel],
        handler: Callable[[list], list],
        sink: str | None = None,
        consumer=None,
        producer=None,
    ):
        self.name = name
        self.settings = settings
        self.codec = codec
        self.source = source
        self.model = model
        self.handler = handler
        self.sink = sink
        self.consumer = consumer or make_consumer(settings, f"{settings.group_prefix}-{name}")
        self.producer = producer or make_producer(settings)
        self.retry_delay = 1.0
        self.stopping = False
        self._delivery_errors: list = []

    def run(self) -> None:
        self.consumer.subscribe([self.source])
        log.info("%s: consuming %s", self.name, self.source)
        try:
            while not self.stopping:
                metrics.beat()
                messages = self.consumer.consume(self.settings.batch_size, timeout=1.0)
                if messages:
                    self.process(messages)
        except Interrupted:
            log.info("%s: stopped while waiting to retry", self.name)
        finally:
            self.producer.flush(10)
            self.consumer.close()

    def stop(self) -> None:
        self.stopping = True

    def process(self, messages: Sequence[Message]) -> None:
        started = time.perf_counter()
        batch, dead = [], 0
        for msg in messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            try:
                batch.append((msg, self._retry(self.codec.decode, msg, self.model)))
            except Interrupted:
                raise
            except Exception as e:
                self.dead_letter(msg, e)
                dead += 1
        results = self._handle([item for _, item in batch])
        for (msg, _), result in zip(batch, results, strict=True):
            if isinstance(result, Failed):
                self.dead_letter(msg, result.error)
                dead += 1
            elif result is not None and self.sink:
                self.send(self.sink, msg.key(), result)
        self._flush()
        self._commit(messages)
        metrics.MESSAGES.labels(self.name, "ok").inc(len(messages) - dead)
        metrics.BATCH_SECONDS.labels(self.name).observe(time.perf_counter() - started)

    def _handle(self, items: list) -> list:
        """배치가 실패하면 한 건씩 다시 돌려 문제 메시지만 골라낸다."""
        if not items:
            return []
        try:
            return self._retry(self.handler, items)
        except Interrupted:
            raise
        except Exception as e:
            if len(items) == 1:
                return [Failed(e)]
            log.warning("%s: batch of %d failed (%r), retrying one by one", self.name, len(items), e)
        results = []
        for item in items:
            try:
                results.extend(self._retry(self.handler, [item]))
            except Interrupted:
                raise
            except Exception as e:
                results.append(Failed(e))
        return results

    def _retry(self, fn, *args):
        delay = self.retry_delay
        while True:
            try:
                return fn(*args)
            except TransientError as e:
                metrics.RETRIES.labels(self.name).inc()
                log.warning("%s: %s (retry in %.1fs)", self.name, e, delay)
                self._pause(delay)
                delay = min(delay * 2, 60)

    def _pause(self, seconds: float) -> None:
        """파티션을 멈춘 채 poll을 계속해 그룹에서 빠지지 않고 기다린다."""
        partitions = self.consumer.assignment()
        self.consumer.pause(partitions)
        try:
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                if self.stopping:
                    raise Interrupted
                metrics.beat()
                msg = self.consumer.poll(min(0.5, seconds))
                if msg is not None and not msg.error():
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        finally:
            try:
                self.consumer.resume(partitions)
            except KafkaException as e:
                log.warning("%s: resume failed: %s", self.name, e)

    def dead_letter(self, msg: Message, error: Exception) -> None:
        log.error("%s: %s[%d]@%d -> dlq: %r", self.name, msg.topic(), msg.partition(), msg.offset(), error)
        metrics.MESSAGES.labels(self.name, "dlq").inc()
        headers = [h for h in msg.headers() or [] if not h[0].startswith("x-")]
        headers += [
            ("x-error", repr(error)[:2000].encode()),
            ("x-stage", self.name.encode()),
            ("x-source-topic", msg.topic().encode()),
            ("x-source-partition", str(msg.partition()).encode()),
            ("x-source-offset", str(msg.offset()).encode()),
        ]
        produce(self.producer, dlq(msg.topic()), msg.key(), msg.value(), headers, self._delivered)

    def send(self, topic: str, key: bytes | None, model: BaseModel) -> None:
        value, headers = self._retry(self.codec.encode, topic, model)
        produce(self.producer, topic, key, value, headers, self._delivered)

    def _delivered(self, err, _msg) -> None:
        if err is not None:
            self._delivery_errors.append(err)

    def _flush(self) -> None:
        pending = self.producer.flush(30)
        if pending or self._delivery_errors:
            errors, self._delivery_errors = self._delivery_errors, []
            raise RuntimeError(f"{self.name}: {pending} messages undelivered, errors={errors[:3]}")

    def _commit(self, messages: Sequence[Message]) -> None:
        offsets = {(m.topic(), m.partition()): m.offset() + 1 for m in messages if not m.error()}
        if not offsets:
            return
        try:
            self.consumer.commit(
                offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False
            )
        except KafkaException as e:
            log.warning("%s: commit failed: %s", self.name, e)
