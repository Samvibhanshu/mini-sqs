# mini-sqs

[![build-and-test](https://github.com/Samvibhanshu/mini-sqs/actions/workflows/ci.yml/badge.svg)](https://github.com/Samvibhanshu/mini-sqs/actions/workflows/ci.yml)

A message queue with Amazon SQS semantics, in plain Java 17: visibility timeouts, receipt handles, dead-letter
queues with redrive, FIFO queues with message groups and deduplication, long polling, and a worker pool with
exponential backoff. No dependencies, just the JDK.

```bash
./build.sh          # compile with -Xlint:all and run all 26 tests (under 10 s)
```

## Why a queue is harder than a list

A consumer can crash halfway through a message. If receiving *removed* the message, it would be lost. So in SQS,
receiving only **hides** it for the visibility timeout, and the consumer deletes it when finished. If the consumer
dies, the message reappears and another consumer gets it. That gives **at-least-once delivery**: nothing is lost,
but a message can be processed twice, so handlers must be idempotent.

Everything else follows from that one decision:

| Problem | What this queue does |
|---|---|
| A slow consumer's message reappears while it is still working | `changeVisibility(handle, ms)` extends the timeout; `0` gives the message back at once |
| An old, slow consumer deletes a message that was since re-delivered to someone else | Each delivery gets a new **receipt handle**; only the latest one can delete. Stale handles return `false` |
| A "poison" message fails forever and keeps coming back | After `maxReceiveCount` deliveries it moves to a **dead-letter queue**. `redriveTo` moves it back after the fix |
| A failing downstream gets hammered by instant retries | The worker sets the next attempt's delay to base × 2^(n−1), capped (exponential backoff) |
| Consumers poll an empty queue in a tight loop | **Long polling**: `receive(max, waitMs)` sleeps until a message arrives or the wait ends; a send wakes it at once |
| Order matters (all events of one order, in sequence) | **FIFO queues**: per message group, strict send order and one message in flight at a time; groups run in parallel |
| A producer retries a send after a timeout | FIFO **deduplication**: a repeated dedup id (or the same body, for content-based dedup) within 5 minutes is accepted but not stored again |
| One busy group monopolises consumers | Groups are served round-robin |
| Messages pile up forever | **Retention**: messages expire a set time after they were *first* sent, even if they moved to the DLQ |

## Usage

```java
MessageQueue dlq = new MessageQueue("orders-dlq", QueueConfig.standard(30_000), Clock.SYSTEM);
MessageQueue q = new MessageQueue("orders", QueueConfig.standard(30_000).withDeadLetterQueue(dlq, 5), Clock.SYSTEM);

q.send("{\"orderId\":42}");
for (ReceivedMessage m : q.receive(10, 20_000)) {      // long poll up to 20 s
    process(m.body());
    q.delete(m.receiptHandle());                       // only after success
}

try (Worker w = new Worker(q, m -> process(m.body()), 8, 1_000, 60_000)) { ... }   // 8 threads, backoff 1 s .. 60 s
```

## How it is built

- **One lock per queue**, and a `Condition` that long-polling receivers wait on. `send` and `changeVisibility` signal it.
- **Standard queue:** a `TreeSet` ordered by "visible at" (ties broken by send order), so a receive takes the earliest visible message in O(log n). A message's position depends on its visible-at time, so it is removed before that changes and re-inserted after. Forgetting that silently corrupts a sorted set; one of the planted bugs below checks for it.
- **FIFO queue:** one deque per group, in send order. Only the head of a group can be delivered, and an in-flight head blocks the rest of its group.
- **Two queues' locks are never held together.** Moving a message to the DLQ happens after the source queue's lock is released, so there is no lock-order deadlock between a queue and its DLQ.
- **Time is injected** (`Clock`). Tests use a manual clock, so "wait 30 seconds" takes no real time and every run is repeatable.

## Tests

| Test class | What it shows |
|---|---|
| `StandardQueueTests` | Visibility timeout, stale receipt handles, delays, batch limits, retention, input validation |
| `FifoQueueTests` | Strict order per group, groups in parallel, dedup window, content-based dedup, round-robin between groups |
| `DeadLetterTests` | Poison messages reach the DLQ without blocking healthy ones; redrive; retention measured from the first send |
| `WorkerTests` | Successes deleted, transient failures retried with backoff, permanent failures dead-lettered |
| `RandomizedInvariantTests` | 600 random sequences of send / receive / delete / change visibility / time passing (standard and FIFO), checking after every step that no message is lost, duplicated or invented, that nothing is delivered while still in flight, and that FIFO order holds |
| `ConcurrencyTests` | 8 producers × 2,500 messages and 8 consumers on real threads: every message deleted exactly once. 20 FIFO groups × 200 messages with 8 competing consumers: each group processed strictly in order. Long polling wakes on send |

**Do the tests catch real bugs?** I planted 11 bugs one at a time, for example: old receipt handles still working, an
off-by-one in the dead-letter count, changing a message's visible-at time while it sits in the sorted set,
retention restarting when a message moves to the DLQ, deduplication never expiring, backoff that does not grow, a send
that does not wake long-polling receivers, and no round-robin between FIFO groups. All 11 are caught.

## Deliberate simplifications

- In memory, single process. Real SQS replicates every message across several servers and availability zones before acknowledging a send.
- A FIFO receive returns at most one message per group. SQS can return several messages of one group in a single batch.
- "Approximate" counts are exact here, because there is one copy of the data.

## Layout

```
src/main/java/dev/samridhi/sqs/
  MessageQueue   send / receive / delete / changeVisibility / redrive, standard and FIFO
  QueueConfig    visibility timeout, DLQ + max receives, retention, FIFO, dedup window
  Worker         consumer thread pool with exponential backoff
  Clock          system clock, or a manual one for tests
src/test/java/dev/samridhi/sqs/   26 tests
```
