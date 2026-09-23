"""A Kafka consumer that survives the things that actually go wrong.

The interesting part of Kafka is not producing and consuming — any quickstart
shows that. It is what happens at a rebalance, on a redelivery, and when one
message in a partition cannot be processed. This module is built around those
three, because they are what separate a demo from something you would run.

The three decisions, and why:

1. **Manual offset commits, after the work.** `enable.auto.commit` commits on a
   timer regardless of whether processing succeeded, so a crash silently loses
   every message between the last commit and the failure. Committing after the
   side effect converts that into at-least-once delivery, which is recoverable.

2. **Idempotent processing keyed on (topic, partition, offset).** At-least-once
   means redelivery WILL happen — at every rebalance, at minimum. A processor
   that is not idempotent turns an ordinary rebalance into duplicated work.

3. **A dead-letter topic, not an infinite retry.** A poison message in a
   partition blocks every message behind it forever, because Kafka has no
   per-message acknowledgement. Moving it aside after N attempts is the only way
   the partition keeps moving.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field

_log = logging.getLogger("processor")
_log.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(logging.Formatter("%(message)s"))
_log.handlers = [_h]
_log.propagate = False


@dataclass(frozen=True)
class Record:
    """One Kafka record. Mirrors the confluent-kafka Message surface we use."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes

    @property
    def coordinate(self) -> str:
        """The natural idempotency key: a record's position is globally unique."""
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass
class Stats:
    consumed: int = 0
    processed: int = 0
    skipped_duplicate: int = 0
    dead_lettered: int = 0
    retried: int = 0
    committed_offsets: dict[tuple[str, int], int] = field(default_factory=dict)


class ProcessingError(Exception):
    """Raised by a handler for a message it cannot process. Triggers retry/DLQ."""


class StreamProcessor:
    """Consume, process idempotently, commit after the work, DLQ the poison.

    `consumer`, `producer` and `store` are injected so the whole thing is
    testable without a broker. In production they are a confluent_kafka
    Consumer/Producer and a durable store; in tests they are in-memory fakes that
    reproduce the semantics that matter, including redelivery.
    """

    def __init__(self, consumer, producer, store, handler, *, dlq_topic: str, max_attempts: int = 3):
        self._consumer = consumer
        self._producer = producer
        self._store = store
        self._handler = handler
        self._dlq_topic = dlq_topic
        self._max_attempts = max_attempts
        self.stats = Stats()

    def process_one(self, rec: Record) -> None:
        self.stats.consumed += 1

        # Idempotency first: a redelivered record must not repeat the side effect.
        if self._store.seen(rec.coordinate):
            self.stats.skipped_duplicate += 1
            self._commit(rec)
            return

        for attempt in range(1, self._max_attempts + 1):
            try:
                self._handler(rec)
            except ProcessingError as e:
                if attempt < self._max_attempts:
                    self.stats.retried += 1
                    _log.info(json.dumps({
                        "event": "retry", "coordinate": rec.coordinate,
                        "attempt": attempt, "error": str(e),
                    }))
                    continue
                # Out of attempts. Move it aside so the partition keeps moving —
                # the alternative is blocking every message behind it forever.
                self._dead_letter(rec, str(e))
                self.stats.dead_lettered += 1
                # The offset is STILL committed. The record is safely in the DLQ;
                # not committing would replay it on the next poll and stall again.
                self._store.mark(rec.coordinate)
                self._commit(rec)
                return
            else:
                self._store.mark(rec.coordinate)
                self.stats.processed += 1
                self._commit(rec)
                return

    def _dead_letter(self, rec: Record, error: str) -> None:
        # Headers carry the provenance: without them a DLQ is a pile of bytes
        # nobody can trace back to a partition and offset.
        self._producer.produce(
            topic=self._dlq_topic,
            key=rec.key,
            value=rec.value,
            headers=[
                ("x-original-topic", rec.topic.encode()),
                ("x-original-partition", str(rec.partition).encode()),
                ("x-original-offset", str(rec.offset).encode()),
                ("x-error", error[:512].encode()),
                ("x-failed-at", str(int(time.time())).encode()),
            ],
        )
        self._producer.flush()
        _log.info(json.dumps({"event": "dead_letter", "coordinate": rec.coordinate, "error": error}))

    def _commit(self, rec: Record) -> None:
        # Kafka commits the NEXT offset to read, not the one just handled. Getting
        # this wrong by one replays the last record of every partition forever.
        self._consumer.commit(rec.topic, rec.partition, rec.offset + 1)
        self.stats.committed_offsets[(rec.topic, rec.partition)] = rec.offset + 1

    def run(self, max_records: int | None = None) -> Stats:
        n = 0
        while max_records is None or n < max_records:
            rec = self._consumer.poll(timeout=1.0)
            if rec is None:
                break
            self.process_one(rec)
            n += 1
        return self.stats
