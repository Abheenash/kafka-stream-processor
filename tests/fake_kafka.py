"""An in-memory broker that reproduces the Kafka behaviours worth testing.

Deliberately NOT a full Kafka: it models per-partition ordering, committed
offsets, redelivery from the last commit, and a rebalance. Those are the
behaviours the processor exists to handle, and none of them need a real broker
to exercise — which is why these tests run in milliseconds with no docker.
"""

from __future__ import annotations


class FakeConsumer:
    def __init__(self, records, drop_commits_from=None):
        # records: list[Record], delivered in order.
        # drop_commits_from: offset at and after which commits are silently lost —
        # the shape of a consumer crashing between the side effect and the commit,
        # which is the ONLY way at-least-once redelivery actually arises.
        self._all = list(records)
        self._cursor = 0
        self._drop_from = drop_commits_from
        self.committed: dict[tuple[str, int], int] = {}
        self.commit_calls: list[tuple[str, int, int]] = []

    def poll(self, timeout=None):
        if self._cursor >= len(self._all):
            return None
        rec = self._all[self._cursor]
        self._cursor += 1
        return rec

    def commit(self, topic, partition, next_offset):
        self.commit_calls.append((topic, partition, next_offset))
        if self._drop_from is not None and next_offset > self._drop_from:
            return  # the commit never reached the broker
        self.committed[(topic, partition)] = next_offset

    def rebalance(self):
        """Simulate a consumer-group rebalance.

        Every record at or after the committed offset is delivered AGAIN. This is
        the ordinary case, not an exotic one — it happens whenever a consumer
        joins, leaves, or is declared dead — and it is why processing must be
        idempotent.
        """
        replay = [
            r for r in self._all[: self._cursor]
            if r.offset >= self.committed.get((r.topic, r.partition), 0)
        ]
        self._all = self._all[: self._cursor] + replay
        self._drop_from = None  # the new consumer's commits land normally
        # cursor stays put: the replayed records come next


class FakeProducer:
    def __init__(self):
        self.produced: list[dict] = []
        self.flushes = 0

    def produce(self, topic, key=None, value=None, headers=None):
        self.produced.append({
            "topic": topic, "key": key, "value": value,
            "headers": dict(headers or []),
        })

    def flush(self):
        self.flushes += 1


class InMemoryStore:
    """Stands in for the durable idempotency store (DynamoDB, Redis, a table)."""

    def __init__(self):
        self._seen: set[str] = set()

    def seen(self, coordinate: str) -> bool:
        return coordinate in self._seen

    def mark(self, coordinate: str) -> None:
        self._seen.add(coordinate)


def records(topic="events", partition=0, values=None, start=0):
    from processor import Record
    return [
        Record(topic=topic, partition=partition, offset=start + i, key=None,
               value=(v if isinstance(v, bytes) else str(v).encode()))
        for i, v in enumerate(values or [])
    ]
