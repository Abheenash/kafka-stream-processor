# Kafka Stream Processor — the three things that actually go wrong

> **Sep 2026:** first release — an at-least-once consumer with idempotent processing, a dead-letter topic and correct offset handling; MSK Serverless with topic-scoped IAM auth. **12 tests: 9 against a fake broker, 3 against a real one** (Redpanda, in CI as a service container). MSK itself deliberately not applied.

Producing and consuming is the easy part, and every quickstart shows it. This
repo is about what happens **at a rebalance, on a redelivery, and when one record
in a partition cannot be processed** — which is where the difference between a
demo and something you would run actually lives.

[**docs/kafka-vs-sqs.md**](docs/kafka-vs-sqs.md) is the companion piece: why
[`job-hunt-command-center`](https://github.com/Abheenash/job-hunt-command-center)
runs its inbox pipeline on **SQS** and should stay there, and what changes when
the model is a log instead of a queue.

## The one that changes your code

**SQS acknowledges individual messages. Kafka does not.**

Kafka commits an **offset** — "I have handled everything up to here". There is no
way to say "everything except record 4,051". So a record that can never be
processed blocks **every record behind it in that partition, forever**, and the
only escape is to move it aside yourself and commit past it.

In SQS that is `maxReceiveCount` and the platform handles it. In Kafka it is code
you have to write, and `src/processor.py` writes it.

## The three decisions

| | Why |
|---|---|
| **Manual commits, after the work** | `enable.auto.commit` commits on a timer whether or not processing succeeded — a crash silently loses everything since the last tick. Committing after the side effect turns that into at-least-once, which is recoverable. |
| **Idempotency keyed on `(topic, partition, offset)`** | At-least-once means redelivery *will* happen. A record's coordinate is globally unique and free — no extra id to generate or store. |
| **A dead-letter topic, not infinite retry** | See above. It is the only way the partition keeps moving. |

And the off-by-one that bites everyone: Kafka commits the offset to **read next**,
not the one just handled. `_commit()` sends `offset + 1`, and
`test_commits_the_next_offset_not_the_current_one` pins it.

## The test worth reading

`test_crash_between_side_effect_and_commit_redelivers_without_repeating_work`.

I wrote it wrong first. My original version processed three records, rebalanced,
and expected redelivery — and got none, because **after a clean commit there is
nothing to replay.** That is correct Kafka behaviour, and the test was wrong.

Redelivery comes from the window between the side effect landing and the commit
reaching the broker. So the fake broker now models exactly that: it drops the
commits for two records, rebalances, and the test asserts the side effects do not
repeat. That window is why idempotency is not optional.

## Testing without a broker

`tests/fake_kafka.py` is not a Kafka. It models per-partition ordering, committed
offsets, lost commits and rebalance replay — the behaviours the processor exists
to handle. They need no broker to exercise, which is why the suite runs in
milliseconds on every push with no docker in CI.

`docker-compose.yml` brings up **Redpanda** (single process, Kafka-API
compatible, ~1s start) for exploring by hand against a real broker.

## Infrastructure

**MSK Serverless**: no broker count, instance type or storage to size — it bills
per GB and per partition-hour, the same reasoning as DynamoDB on-demand and
Firestore elsewhere in these projects.

It supports **only IAM authentication**, which is a feature: no SASL/SCRAM
password to store, rotate or leak. The policy in `terraform/main.tf` grants read
on exactly one topic, write on exactly one dead-letter topic, and membership of
exactly one consumer group — finer-grained than most Kafka setups bother with.

## Status

**Validated, not applied.** MSK Serverless bills per partition-hour whether or
not anything flows, so an idle cluster quietly costs money. `terraform validate`
is clean, 3 terraform tests and 9 unit tests pass, checkov is clean (46 checks),
and the part that is actually hard — the consumer's behaviour — is proven against
the fake broker rather than asserted in prose.

```bash
python -m pytest tests -q            # 9 unit tests, no broker (integration skips)
docker compose up -d                 # Redpanda, ~2s
python -m pytest tests -q            # now 12 — the 3 integration tests run too
terraform -chdir=terraform test      # mocked provider
```

The integration tests are the ones that prove the *fake was faithful*: that the
adapter really maps confluent-kafka's surface onto the processor's interface, and
that a second consumer in the same group resumes **after** the committed offset
rather than replaying. They skip automatically with no broker, so the fast path
stays fast.

## Not affiliated with Apache or Confluent — a personal learning + portfolio project by
[Rajolu Abheenash](https://abheenash.com).
