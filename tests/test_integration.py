"""The processor against a REAL broker.

The unit tests prove the logic against a fake. These prove the fake was faithful —
that the adapter really does map confluent-kafka's surface onto the interface the
processor expects, and that the semantics the fake models (offset commits,
redelivery from the committed position, DLQ writes) are the semantics a broker
actually has.

Skipped automatically when no broker is reachable, so `pytest` still works in a
few seconds with no docker:

    docker compose up -d
    pytest tests -q
"""
from __future__ import annotations

import time
import uuid

import pytest

pytest.importorskip("confluent_kafka")
from adapters import ConfluentConsumer, ConfluentProducer
from processor import ProcessingError, StreamProcessor

BOOTSTRAP = "127.0.0.1:9092"


def _broker_up() -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        md = AdminClient({"bootstrap.servers": BOOTSTRAP, "socket.timeout.ms": 2000}).list_topics(timeout=3)
        return len(md.brokers) > 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _broker_up(), reason="no Kafka broker at 127.0.0.1:9092 — run `docker compose up -d`"
)


class Store:
    def __init__(self):
        self._seen = set()

    def seen(self, c):
        return c in self._seen

    def mark(self, c):
        self._seen.add(c)


@pytest.fixture
def topics():
    from confluent_kafka.admin import AdminClient, NewTopic

    suffix = uuid.uuid4().hex[:8]
    main, dlq = f"events-{suffix}", f"events-{suffix}.dlq"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    # One partition: these tests assert on offsets, and ordering across
    # partitions is not guaranteed by design.
    futs = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in (main, dlq)])
    for f in futs.values():
        f.result(timeout=20)
    yield main, dlq
    admin.delete_topics([main, dlq])


def publish(topic, values):
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for v in values:
        p.produce(topic=topic, value=v.encode())
    assert p.flush(15) == 0


def consumer_for(topic, group):
    from confluent_kafka import Consumer

    return ConfluentConsumer(
        Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            # The entire design depends on this being off.
            "enable.auto.commit": False,
        }),
        [topic],
    )


def producer():
    from confluent_kafka import Producer

    return ConfluentProducer(Producer({"bootstrap.servers": BOOTSTRAP}))


def test_processes_a_real_topic_end_to_end(topics):
    main, dlq = topics
    publish(main, ["a", "b", "c"])

    seen = []
    c = consumer_for(main, f"g-{uuid.uuid4().hex[:6]}")
    proc = StreamProcessor(c, producer(), Store(), lambda r: seen.append(r.value), dlq_topic=dlq)
    try:
        proc.run(max_records=3)
    finally:
        c.close()

    assert seen == [b"a", b"b", b"c"]
    assert proc.stats.processed == 3


def test_a_new_consumer_resumes_after_the_committed_offset(topics):
    """This is what the fake models as `rebalance()`. Against a real broker: a
    second consumer in the SAME group must not re-read what the first committed."""
    main, dlq = topics
    publish(main, ["1", "2", "3", "4"])
    group = f"g-{uuid.uuid4().hex[:6]}"

    first_seen = []
    c1 = consumer_for(main, group)
    p1 = StreamProcessor(c1, producer(), Store(), lambda r: first_seen.append(r.value), dlq_topic=dlq)
    try:
        p1.run(max_records=2)
    finally:
        c1.close()
    assert first_seen == [b"1", b"2"]

    time.sleep(1)  # let the group rebalance after the first consumer leaves

    second_seen = []
    c2 = consumer_for(main, group)
    p2 = StreamProcessor(c2, producer(), Store(), lambda r: second_seen.append(r.value), dlq_topic=dlq)
    try:
        p2.run(max_records=2)
    finally:
        c2.close()

    # The committed offset is honoured: no replay of 1 and 2.
    assert second_seen == [b"3", b"4"], f"expected resume after commit, got {second_seen}"


def test_poison_record_reaches_the_dlq_on_a_real_broker(topics):
    main, dlq = topics
    publish(main, ["ok", "poison", "after"])

    def handler(rec):
        if rec.value == b"poison":
            raise ProcessingError("cannot parse")

    c = consumer_for(main, f"g-{uuid.uuid4().hex[:6]}")
    proc = StreamProcessor(c, producer(), Store(), handler, dlq_topic=dlq, max_attempts=2)
    try:
        proc.run(max_records=3)
    finally:
        c.close()

    assert proc.stats.dead_lettered == 1
    assert proc.stats.processed == 2, "records behind the poison one must still be processed"

    # And it really is on the DLQ topic, with its provenance headers.
    from confluent_kafka import Consumer

    dc = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-{uuid.uuid4().hex[:6]}",
                   "auto.offset.reset": "earliest", "enable.auto.commit": False})
    dc.subscribe([dlq])
    msg = None
    for _ in range(20):
        msg = dc.poll(1.0)
        if msg is not None and not msg.error():
            break
    dc.close()
    assert msg is not None and msg.value() == b"poison"
    headers = dict(msg.headers() or [])
    assert headers["x-original-topic"] == main.encode()
    assert b"cannot parse" in headers["x-error"]
