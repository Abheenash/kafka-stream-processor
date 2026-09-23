# Kafka, SQS and Kinesis — when the log model actually matters

`job-hunt-command-center` runs an event-driven pipeline on **SQS**:
`EventBridge → Scanner → SQS(+DLQ) → Dispatcher → Step Functions`. It works, and
for that workload it is the right choice. This repo is the same shape of problem
on **Kafka**, and the differences are not cosmetic.

| | SQS | Kinesis | Kafka |
|---|---|---|---|
| Model | queue — a message is **removed** when acked | log | **log — records stay for the retention period** |
| Ack | **per message** | per shard checkpoint | per partition **offset** |
| Poison message | isolated by `maxReceiveCount` → DLQ | **blocks the shard** | **blocks the partition** |
| Ordering | FIFO queues only | per shard | **per partition** |
| Replay | impossible — it's gone | within retention | **rewind the offset, any consumer, any time** |
| Many consumers | compete for the same message | per-shard limits | **independent consumer groups, own offsets** |
| Scaling unit | none — it just absorbs | shards | partitions |
| Ops | none | little | real, unless serverless |

## The one difference that changes your code

**SQS acknowledges individual messages. Kafka does not.**

In SQS, one unprocessable message is deleted or left to age into the DLQ after
`maxReceiveCount`, and everything else keeps flowing. That is a *queue*
behaviour, and it is why the SQS pipeline in `job-hunt-command-center` needed
almost no poison-message handling — the platform does it.

Kafka commits an **offset**: "I have handled everything up to here." There is no
way to say "everything except record 4,051". So a record that can never be
processed blocks **every record behind it in that partition, forever**, and the
only escape is to move it aside yourself and commit past it. That is
`_dead_letter()` in `src/processor.py`, and it is not optional — it is what the
log model costs you.

## The second difference: redelivery is normal, not exceptional

Because the offset is committed *after* the work, there is always a window where
the side effect has landed and the commit has not. A consumer that dies in that
window has done the work and not recorded it, so the next consumer gets the
record again.

This is not an edge case. It happens on every crash, every deploy, every
rebalance that catches a consumer mid-batch. So processing has to be idempotent,
and the natural key is the record's own coordinate — `(topic, partition, offset)`
is globally unique and free.

`tests/test_processor.py` models exactly this: it drops the commits for two
records, rebalances, and asserts the side effects do not repeat.

## When I would still choose SQS

- The work is independent per message and order does not matter
- Per-message retry and a platform-managed DLQ are what you want
- Nobody will ever need to replay
- You do not want to operate, or pay for, a cluster

That describes the `job-hunt-command-center` inbox pipeline precisely, which is
why it is on SQS and should stay there.

## When Kafka earns it

- **Replay** — a new consumer needs last week's events, or a bug means
  reprocessing yesterday
- **Several independent consumers** of the same stream, each at its own position
- **Ordering per key** across a high-throughput stream
- Retention as a feature: the log *is* the record, not a transport

## Why MSK Serverless

Provisioned MSK asks you to pick a broker count, instance type and storage before
you know the workload. Serverless bills per GB and per partition-hour, which
matches a bursty stream — the same reasoning as DynamoDB on-demand, Cosmos
serverless and Firestore in the other repos.

It also only supports **IAM authentication**, which is a feature: no SASL/SCRAM
password to store, rotate or leak, and authorisation becomes an IAM policy. The
one in `terraform/main.tf` grants read on exactly one topic, write on exactly one
dead-letter topic, and membership of exactly one consumer group.

## Status

**Not applied.** MSK Serverless bills per partition-hour whether or not anything
is flowing, so an idle cluster quietly costs money. The Terraform is validated
and unit-tested; the processor's behaviour — the part that is actually hard — is
proven against a fake broker that reproduces per-partition ordering, committed
offsets, lost commits and rebalance replay. Those tests run in milliseconds and
need no docker, which is why they run on every push.
