"""Thin adapters mapping confluent-kafka onto the interface StreamProcessor uses.

The processor takes `consumer`, `producer` and `store` as constructor arguments
precisely so its logic can be tested without a broker. These adapters are the
other half of that bargain: they are the only code that touches the real client,
they contain no logic, and `tests/test_integration.py` runs the processor through
them against an actual broker to prove the abstraction is faithful.

Keeping them this thin is deliberate. An adapter with branching in it is a place
for a bug that the unit tests cannot see and the integration tests are too slow
to cover exhaustively.
"""

from __future__ import annotations

from processor import Record


class ConfluentConsumer:
    """Wraps confluent_kafka.Consumer. Manual commit only."""

    def __init__(self, consumer, topics: list[str]):
        self._c = consumer
        self._c.subscribe(topics)

    def poll(self, timeout: float = 1.0):
        msg = self._c.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"kafka error: {msg.error()}")
        return Record(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key(),
            value=msg.value(),
        )

    def commit(self, topic: str, partition: int, next_offset: int) -> None:
        from confluent_kafka import TopicPartition

        # asynchronous=False: the point of manual commits is knowing the offset
        # actually landed. A fire-and-forget commit reintroduces the very window
        # this design exists to close.
        self._c.commit(offsets=[TopicPartition(topic, partition, next_offset)], asynchronous=False)

    def close(self) -> None:
        self._c.close()


class ConfluentProducer:
    """Wraps confluent_kafka.Producer."""

    def __init__(self, producer):
        self._p = producer

    def produce(self, topic, key=None, value=None, headers=None):
        self._p.produce(topic=topic, key=key, value=value, headers=headers)

    def flush(self, timeout: float = 10.0) -> int:
        return self._p.flush(timeout)
