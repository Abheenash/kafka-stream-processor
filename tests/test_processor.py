"""The three failure modes this processor exists to handle.

Every test here corresponds to something that goes wrong in production and is
invisible in a quickstart: rebalance redelivery, a poison message blocking a
partition, and committing the wrong offset.
"""
import pytest

from fake_kafka import FakeConsumer, FakeProducer, InMemoryStore, records
from processor import ProcessingError, Record, StreamProcessor


def build(recs, handler, max_attempts=3):
    c, p, s = FakeConsumer(recs), FakeProducer(), InMemoryStore()
    return StreamProcessor(c, p, s, handler, dlq_topic="events.dlq", max_attempts=max_attempts), c, p, s


# --- 1. offsets ------------------------------------------------------------

def test_commits_the_next_offset_not_the_current_one():
    """Kafka commits the offset to READ NEXT. Off by one here and the last record
    of every partition replays forever."""
    recs = records(values=["a", "b", "c"])
    proc, c, _, _ = build(recs, lambda r: None)
    proc.run()
    assert c.commit_calls == [("events", 0, 1), ("events", 0, 2), ("events", 0, 3)]
    assert c.committed[("events", 0)] == 3


def test_offset_is_not_committed_before_the_work():
    """Auto-commit would commit on a timer regardless of success. A handler that
    raises must not advance the offset past the record it failed on — unless the
    record has been dead-lettered, which the DLQ test covers."""
    seen = []

    def handler(rec):
        seen.append(rec.offset)
        raise ProcessingError("always fails")

    proc, c, p, _ = build(records(values=["a"]), handler, max_attempts=1)
    proc.run()
    # It went to the DLQ, so the offset advances — the record is safe elsewhere.
    assert p.produced and p.produced[0]["topic"] == "events.dlq"
    assert c.committed[("events", 0)] == 1


# --- 2. redelivery / idempotency -------------------------------------------

def test_crash_between_side_effect_and_commit_redelivers_without_repeating_work():
    """The headline test.

    At-least-once redelivery does not come from a rebalance on its own — after a
    clean commit there is nothing to replay. It comes from the window between the
    side effect landing and the commit reaching the broker. A consumer that dies
    in that window has done the work and not recorded it, so the next consumer
    gets the record again.

    That is precisely why processing is keyed on (topic, partition, offset): the
    replay is unavoidable, so it has to be harmless.
    """
    effects = []
    recs = records(values=["a", "b", "c"])
    c, p, s = FakeConsumer(recs, drop_commits_from=1), FakeProducer(), InMemoryStore()
    proc = StreamProcessor(c, p, s, lambda r: effects.append(r.coordinate),
                           dlq_topic="events.dlq", max_attempts=3)

    proc.run(max_records=3)
    assert len(effects) == 3
    # Offsets 1 and 2 were processed but their commits were lost.
    assert c.committed[("events", 0)] == 1

    c.rebalance()
    proc.run(max_records=3)

    assert len(effects) == 3, "redelivered records must not re-run the side effect"
    assert proc.stats.skipped_duplicate == 2
    assert c.committed[("events", 0)] == 3, "the replay must commit through, or it stalls forever"


def test_duplicate_is_still_committed():
    """A skipped duplicate must advance the offset, or the consumer stalls on it."""
    recs = records(values=["a"])
    proc, c, _, store = build(recs, lambda r: None)
    store.mark("events:0:0")
    proc.run()
    assert proc.stats.skipped_duplicate == 1
    assert c.committed[("events", 0)] == 1


def test_idempotency_key_is_the_record_coordinate():
    r = Record(topic="t", partition=7, offset=42, key=None, value=b"x")
    assert r.coordinate == "t:7:42"


# --- 3. poison messages ----------------------------------------------------

def test_poison_message_goes_to_the_dlq_and_the_partition_keeps_moving():
    """Kafka has no per-message ack. A record that can never be processed blocks
    every record behind it in its partition — forever — unless it is moved aside."""
    def handler(rec):
        if rec.value == b"poison":
            raise ProcessingError("cannot parse")

    recs = records(values=["ok1", "poison", "ok2"])
    proc, c, prod, _ = build(recs, handler, max_attempts=2)
    proc.run()

    assert proc.stats.dead_lettered == 1
    assert proc.stats.processed == 2, "records behind the poison one must still be processed"
    assert c.committed[("events", 0)] == 3
    assert prod.produced[0]["topic"] == "events.dlq"


def test_dlq_record_carries_its_provenance():
    """A DLQ without headers is a pile of bytes nobody can trace back."""
    def handler(rec):
        raise ProcessingError("boom")

    proc, _, p, _ = build(records(values=["bad"]), handler, max_attempts=1)
    proc.run()

    h = p.produced[0]["headers"]
    assert h["x-original-topic"] == b"events"
    assert h["x-original-partition"] == b"0"
    assert h["x-original-offset"] == b"0"
    assert b"boom" in h["x-error"]
    assert p.flushes >= 1, "the DLQ write must be flushed before the offset is committed"


def test_retries_before_giving_up():
    attempts = []

    def handler(rec):
        attempts.append(1)
        if len(attempts) < 3:
            raise ProcessingError("transient")

    proc, _, prod, _ = build(records(values=["flaky"]), handler, max_attempts=3)
    proc.run()

    assert len(attempts) == 3
    assert proc.stats.processed == 1
    assert proc.stats.retried == 2
    assert not prod.produced, "a message that eventually succeeds must not reach the DLQ"


def test_a_handler_raising_an_unexpected_error_is_not_swallowed():
    """ProcessingError means 'this message is bad'. Anything else is a bug in the
    handler and must surface, not be quietly dead-lettered."""
    def handler(rec):
        raise ZeroDivisionError("a real bug")

    proc, _, _, _ = build(records(values=["x"]), handler)
    with pytest.raises(ZeroDivisionError):
        proc.run()
