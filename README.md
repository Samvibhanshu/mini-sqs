# mini-sqs

[![build-and-test](https://github.com/Samvibhanshu/mini-sqs/actions/workflows/ci.yml/badge.svg)](https://github.com/Samvibhanshu/mini-sqs/actions/workflows/ci.yml)

A message queue with Amazon SQS semantics, in plain Java 17: visibility timeouts, receipt handles, dead-letter
queues with redrive, FIFO queues with message groups and deduplication, long polling, and a worker pool with
exponential backoff. No dependencies, just the JDK.

It runs as a library, or as a **network service that speaks SQS's own JSON protocol**, so the official AWS SDKs can
use it as a local SQS. CI checks that with boto3 on every push.

```bash
./build.sh                                           # compile with -Xlint:all and run all 39 tests (about 20 s)
java -cp out/main dev.samridhi.sqs.server.Main      # local SQS endpoint on http://127.0.0.1:9324
pip install boto3 && python compat/boto3_check.py   # the official AWS SDK against it: 27 checks
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

## As a service: the SQS wire protocol

```python
import boto3
sqs = boto3.client("sqs", endpoint_url="http://127.0.0.1:9324", region_name="us-east-1",
                   aws_access_key_id="local", aws_secret_access_key="local")
url = sqs.create_queue(QueueName="orders")["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody='{"orderId": 42}')
for m in sqs.receive_message(QueueUrl=url, WaitTimeSeconds=20).get("Messages", []):
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
```

The AWS SDKs talk to SQS with `POST /`, an `X-Amz-Target: AmazonSQS.<Action>` header and a JSON body. `SqsServer`
answers the same way, down to the details SDKs check:

- **MD5 checksums.** `MD5OfMessageBody` and `MD5OfBody` are computed over the UTF-8 bytes; botocore verifies both and
  refuses the response if they differ (a planted bug that hashed UTF-16 is caught by the tests).
- **Errors by name.** `{"__type": "com.amazonaws.sqs#QueueDoesNotExist", ...}` plus the older code in the
  `x-amzn-query-error` header, so `except sqs.exceptions.QueueDoesNotExist` works in boto3.
- **SQS's validation.** Queue names (`.fifo` for FIFO), attribute ranges, FIFO needing a group id and a deduplication id
  (unless content-based), the characters SQS allows in a body, batches of 1–10 entries with distinct ids and at most
  256 KB in total, each entry succeeding or failing on its own.

| Supported | Not supported (answered with `UnsupportedOperation`) |
|---|---|
| CreateQueue, GetQueueUrl, ListQueues, DeleteQueue, GetQueueAttributes, SendMessage(Batch), ReceiveMessage (long polling, per-receive visibility), DeleteMessage(Batch), ChangeMessageVisibility(Batch), ListDeadLetterSourceQueues, StartMessageMoveTask (redrive) | SetQueueAttributes, PurgeQueue, tags, permissions, message attributes, CancelMessageMoveTask / ListMessageMoveTasks |

**Long polling holds a thread.** A receive with `WaitTimeSeconds` parks its request thread until a message arrives
or the wait ends. At most 256 receives wait at once; past that, a receive is answered immediately with whatever is
there (an empty answer is always legal in SQS), so idle consumers can never use up the server. A test fills the cap
and checks the next receive comes back at once.

**Every response took 40 ms** until the server set `TCP_NODELAY`: the JDK's HTTP server writes headers and body
separately, and with Nagle's algorithm on, the body waited for the client's delayed ACK. Now about 2 ms; a test
guards it. (The same bug was in order-service.)

A small Java client, `SqsClient`, wraps the same protocol; the tests use it.

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
| `HttpServerTests` | The service over real HTTP: queue lifecycle and SQS's name and attribute rules, checksums, visibility and stale handles, delays, a long poll woken by a send, the long-poll cap, FIFO rules, dead-letter queue and redrive, per-entry batch results, malformed and oversized requests, 4 producers and 8 consumers moving 1,200 messages with nothing lost or duplicated, and response latency |
| `compat/boto3_check.py` | The official AWS SDK for Python against a running server: 27 checks, run in CI |

**Do the tests catch real bugs?** I planted 11 bugs one at a time, for example: old receipt handles still working, an
off-by-one in the dead-letter count, changing a message's visible-at time while it sits in the sorted set,
retention restarting when a message moves to the DLQ, deduplication never expiring, backoff that does not grow, a send
that does not wake long-polling receivers, and no round-robin between FIFO groups. All 11 are caught.

For the HTTP layer, 6 more: checksums over the wrong bytes, no long-poll cap, a per-receive visibility timeout
ignored, FIFO sends without a deduplication id accepted, `WaitTimeSeconds` ignored, and one bad handle failing a whole
delete batch. All 6 are caught.

## Deliberate simplifications

- In memory, single process: a restart loses every message. Real SQS replicates every message across several servers and availability zones before acknowledging a send.
- No authentication: request signatures are ignored, so the server binds to 127.0.0.1 by default.
- A stale or already-used receipt handle is an error here (`ReceiptHandleIsInvalid`); real SQS is more forgiving
  with some of these.
- A FIFO receive returns at most one message per group. SQS can return several messages of one group in a single batch.
- "Approximate" counts are exact here, because there is one copy of the data.

## Layout

```
src/main/java/dev/samridhi/sqs/
  MessageQueue   send / receive / delete / changeVisibility / redrive, standard and FIFO
  QueueConfig    visibility timeout, DLQ + max receives, retention, FIFO, dedup window
  Worker         consumer thread pool with exponential backoff
  Clock          system clock, or a manual one for tests
src/main/java/dev/samridhi/sqs/server/   SqsServer (HTTP, SQS JSON protocol), QueueRegistry (queues, attributes), Main
src/main/java/dev/samridhi/sqs/client/   SqsClient
src/test/java/dev/samridhi/sqs/   39 tests          compat/boto3_check.py   the AWS SDK for Python against the server
```
