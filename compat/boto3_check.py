"""Runs the official AWS SDK for Python (boto3) against a running mini-sqs.

    java -cp out/main dev.samridhi.sqs.server.Main &        # in another terminal
    pip install boto3 && python compat/boto3_check.py [endpoint]

botocore checks the MD5 of every body it sends and receives, parses every response with SQS's own service model, and
maps errors to its modelled exception classes, so this is a strict test of the wire format.
"""
import json
import sys
import time
import uuid

import boto3
from botocore.config import Config

endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9324"
sqs = boto3.client("sqs", endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="local",
                   aws_secret_access_key="local", config=Config(retries={"max_attempts": 1}))
run = uuid.uuid4().hex[:8]
checks = 0


def check(cond, what):
    global checks
    if not cond:
        raise SystemExit("FAILED: " + what)
    checks += 1
    print("  ok  " + what)


def expect_error(exc_class, fn, what):
    try:
        fn()
    except exc_class:
        check(True, what)
        return
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"FAILED: {what}: got {type(e).__name__}: {e}")
    raise SystemExit("FAILED: " + what + ": no error")


# queues
url = sqs.create_queue(QueueName=f"orders-{run}", Attributes={"VisibilityTimeout": "2"})["QueueUrl"]
check(url.endswith(f"/000000000000/orders-{run}"), "create_queue returns a queue URL")
check(sqs.create_queue(QueueName=f"orders-{run}", Attributes={"VisibilityTimeout": "2"})["QueueUrl"] == url,
      "create_queue is idempotent for the same attributes")
expect_error(sqs.exceptions.QueueNameExists, lambda: sqs.create_queue(QueueName=f"orders-{run}", Attributes={"VisibilityTimeout": "5"}),
             "same name, different attributes -> QueueNameExists")
check(sqs.get_queue_url(QueueName=f"orders-{run}")["QueueUrl"] == url, "get_queue_url")
check(url in sqs.list_queues(QueueNamePrefix="orders-")["QueueUrls"], "list_queues with a prefix")
expect_error(sqs.exceptions.QueueDoesNotExist, lambda: sqs.get_queue_url(QueueName="no-such-queue"),
             "unknown queue -> QueueDoesNotExist")

# send, receive (botocore verifies MD5OfMessageBody / MD5OfBody), delete
body = "héllo, wörld ✓ " + run
sent = sqs.send_message(QueueUrl=url, MessageBody=body)
check(len(sent["MessageId"]) > 0, "send_message (MD5 of the body verified by botocore)")
got = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, MessageSystemAttributeNames=["All"])["Messages"]
check(len(got) == 1 and got[0]["Body"] == body, "receive_message returns the body (MD5 verified)")
check(got[0]["Attributes"]["ApproximateReceiveCount"] == "1", "ApproximateReceiveCount attribute")
check("Messages" not in sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0), "the message is hidden while in flight")
time.sleep(2.2)
again = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1, MessageSystemAttributeNames=["All"])["Messages"][0]
check(again["MessageId"] == got[0]["MessageId"] and again["Attributes"]["ApproximateReceiveCount"] == "2",
      "after the visibility timeout it is delivered again")
expect_error(sqs.exceptions.ReceiptHandleIsInvalid, lambda: sqs.delete_message(QueueUrl=url, ReceiptHandle=got[0]["ReceiptHandle"]),
             "the first delivery's receipt handle no longer works")
sqs.delete_message(QueueUrl=url, ReceiptHandle=again["ReceiptHandle"])
check("Messages" not in sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0), "delete_message with the latest handle")

# long polling: a send wakes a waiting receive
t0 = time.time()
check("Messages" not in sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1), "an empty long poll returns empty")
check(time.time() - t0 >= 0.9, "...after waiting the full WaitTimeSeconds")

# batches
res = sqs.send_message_batch(QueueUrl=url, Entries=[{"Id": f"m{i}", "MessageBody": f"batch {i}"} for i in range(10)])
check(len(res["Successful"]) == 10 and not res.get("Failed"), "send_message_batch of 10 (MD5s verified per entry)")
handles = []
while len(handles) < 10:
    for m in sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", []):
        handles.append(m["ReceiptHandle"])
res = sqs.delete_message_batch(QueueUrl=url, Entries=[{"Id": f"d{i}", "ReceiptHandle": h} for i, h in enumerate(handles[:9])] +
                                [{"Id": "bad", "ReceiptHandle": "not-a-handle"}])
check(len(res["Successful"]) == 9 and [f["Id"] for f in res["Failed"]] == ["bad"],
      "delete_message_batch: 9 deleted, the bad handle reported on its own")
sqs.delete_message(QueueUrl=url, ReceiptHandle=handles[9])
expect_error(sqs.exceptions.TooManyEntriesInBatchRequest,
             lambda: sqs.send_message_batch(QueueUrl=url, Entries=[{"Id": f"m{i}", "MessageBody": "x"} for i in range(11)]),
             "11 batch entries -> TooManyEntriesInBatchRequest")
expect_error(sqs.exceptions.BatchEntryIdsNotDistinct,
             lambda: sqs.send_message_batch(QueueUrl=url, Entries=[{"Id": "same", "MessageBody": "x"}] * 2),
             "repeated batch ids -> BatchEntryIdsNotDistinct")

# attributes
attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
check(attrs["VisibilityTimeout"] == "2" and attrs["QueueArn"].endswith(f":orders-{run}"), "get_queue_attributes")
check(attrs["ApproximateNumberOfMessages"] == "0", "ApproximateNumberOfMessages")

# FIFO: order per group and deduplication
fifo = sqs.create_queue(QueueName=f"events-{run}.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
for i in range(5):
    sqs.send_message(QueueUrl=fifo, MessageBody=f"e{i}", MessageGroupId="order-42", MessageDeduplicationId=f"d{i}")
sqs.send_message(QueueUrl=fifo, MessageBody="e0 again", MessageGroupId="order-42", MessageDeduplicationId="d0")
seen = []
while len(seen) < 5:
    batch = sqs.receive_message(QueueUrl=fifo, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
    for m in batch:
        seen.append(m["Body"])
        sqs.delete_message(QueueUrl=fifo, ReceiptHandle=m["ReceiptHandle"])
check(seen == [f"e{i}" for i in range(5)], "FIFO: one group comes back in send order, the duplicate is dropped")
expect_error(sqs.exceptions.ClientError, lambda: sqs.send_message(QueueUrl=fifo, MessageBody="x", MessageGroupId="g"),
             "FIFO without a deduplication id (and no content-based dedup) is refused")

# dead-letter queue and redrive
dlq = sqs.create_queue(QueueName=f"orders-dlq-{run}")["QueueUrl"]
dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
src = sqs.create_queue(QueueName=f"work-{run}", Attributes={
    "VisibilityTimeout": "1", "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"})})["QueueUrl"]
sqs.send_message(QueueUrl=src, MessageBody="poison")
for _ in range(2):
    sqs.receive_message(QueueUrl=src, WaitTimeSeconds=2)            # received, never deleted
    time.sleep(1.1)
sqs.receive_message(QueueUrl=src, WaitTimeSeconds=0)                # the third receive moves it to the DLQ
check(sqs.receive_message(QueueUrl=dlq, WaitTimeSeconds=1)["Messages"][0]["Body"] == "poison",
      "after maxReceiveCount deliveries the message is in the dead-letter queue")
check(sqs.list_dead_letter_source_queues(QueueUrl=dlq)["queueUrls"] == [src], "list_dead_letter_source_queues")
sqs.start_message_move_task(SourceArn=dlq_arn)
moved = sqs.receive_message(QueueUrl=src, WaitTimeSeconds=1).get("Messages", [])
check(len(moved) == 1 and moved[0]["Body"] == "poison", "start_message_move_task moves it back to the source queue")

for q in (url, fifo, dlq, src):
    sqs.delete_queue(QueueUrl=q)
expect_error(sqs.exceptions.QueueDoesNotExist, lambda: sqs.send_message(QueueUrl=url, MessageBody="x"),
             "a deleted queue -> QueueDoesNotExist")
print(f"\nboto3 {boto3.__version__}: all {checks} checks passed")
