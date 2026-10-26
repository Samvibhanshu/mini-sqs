package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.client.SqsClient;
import dev.samridhi.sqs.server.QueueRegistry;
import dev.samridhi.sqs.server.SqsServer;
import dev.samridhi.sqs.testkit.Test;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** The queue as a network service: every test talks to a real server over HTTP with the SQS JSON protocol. */
public class HttpServerTests {

    private static final class Local implements AutoCloseable {
        final Clock.Manual clock;
        final SqsServer server;
        final SqsClient client;
        final String endpoint;

        Local(Clock.Manual manual, int maxLongPolls) throws Exception {
            clock = manual;
            server = new SqsServer(new QueueRegistry(manual != null ? manual : Clock.SYSTEM), maxLongPolls);
            endpoint = "http://127.0.0.1:" + server.start("127.0.0.1", 0);
            client = new SqsClient(endpoint);
        }

        static Local realTime() throws Exception {
            return new Local(null, SqsServer.MAX_LONG_POLLS);
        }

        static Local manualTime() throws Exception {
            return new Local(new Clock.Manual(1_000_000), SqsServer.MAX_LONG_POLLS);
        }

        @Override
        public void close() {
            server.stop();
        }
    }

    private static String expectError(String code, Runnable r) {
        try {
            r.run();
        } catch (SqsClient.Error e) {
            assertEquals(code, e.code);
            return e.getMessage();
        }
        throw new AssertionError("expected " + code);
    }

    private static String md5(String s) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void queuesAreCreatedFoundListedAndDeleted() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("orders", Map.of("VisibilityTimeout", "45"));
            assertTrue(url.endsWith("/000000000000/orders"), url);
            assertEquals(url, s.client.createQueue("orders", Map.of("VisibilityTimeout", "45")));     // idempotent
            expectError("QueueNameExists", () -> s.client.createQueue("orders", Map.of("VisibilityTimeout", "10")));
            assertEquals(url, s.client.queueUrl("orders"));
            s.client.createQueue("other", Map.of());
            assertEquals(List.of(url), s.client.listQueues("ord"));
            assertEquals("45", s.client.attributes(url).get("VisibilityTimeout"));
            s.client.deleteQueue(url);
            expectError("QueueDoesNotExist", () -> s.client.queueUrl("orders"));
            expectError("QueueDoesNotExist", () -> s.client.send(url, "x"));
        }
    }

    @Test
    void queueNamesAndAttributesAreCheckedLikeSqs() throws Exception {
        try (Local s = Local.realTime()) {
            expectError("InvalidParameterValue", () -> s.client.createQueue("has space", Map.of()));
            expectError("InvalidParameterValue", () -> s.client.createQueue("x".repeat(81), Map.of()));
            expectError("InvalidParameterValue", () -> s.client.createQueue("plain", Map.of("FifoQueue", "true")));   // FIFO needs .fifo
            expectError("InvalidAttributeName", () -> s.client.createQueue("q", Map.of("NoSuchThing", "1")));
            expectError("InvalidParameterValue", () -> s.client.createQueue("q", Map.of("VisibilityTimeout", "43201")));
            expectError("InvalidParameterValue", () -> s.client.createQueue("q", Map.of("RedrivePolicy",
                    "{\"deadLetterTargetArn\":\"arn:aws:sqs:local:000000000000:missing\",\"maxReceiveCount\":\"3\"}")));
            Map<String, String> a = s.client.attributes(s.client.createQueue("q.fifo", Map.of()));
            assertEquals("true", a.get("FifoQueue"));
            assertEquals("arn:aws:sqs:local:000000000000:q.fifo", a.get("QueueArn"));
        }
    }

    @Test
    void sendReceiveDeleteWithChecksums() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("q", Map.of());
            String body = "ünïcödé ✓ {\"json\": true}\n";
            Map<String, Object> sent = s.client.call("SendMessage", Map.of("QueueUrl", url, "MessageBody", body));
            assertEquals(md5(body), sent.get("MD5OfMessageBody"));
            Map<String, Object> got = s.client.call("ReceiveMessage", Map.of("QueueUrl", url));
            Map<?, ?> m = (Map<?, ?>) ((List<?>) got.get("Messages")).get(0);
            assertEquals(body, m.get("Body"));
            assertEquals(md5(body), m.get("MD5OfBody"));
            assertEquals(sent.get("MessageId"), m.get("MessageId"));
            s.client.delete(url, (String) m.get("ReceiptHandle"));
            expectError("ReceiptHandleIsInvalid", () -> s.client.delete(url, (String) m.get("ReceiptHandle")));
            assertEquals(0, s.client.receive(url, 10, 0).size());
        }
    }

    @Test
    void visibilityTimeoutAndStaleHandlesOverHttp() throws Exception {
        try (Local s = Local.manualTime()) {
            String url = s.client.createQueue("q", Map.of("VisibilityTimeout", "30"));
            s.client.send(url, "job");
            SqsClient.Message first = s.client.receive(url, 1, 0).get(0);
            assertEquals(0, s.client.receive(url, 1, 0).size());                  // hidden while in flight
            s.clock.advance(30_000);
            SqsClient.Message second = s.client.receive(url, 1, 0).get(0);
            assertEquals(first.messageId(), second.messageId());
            assertEquals(2, second.receiveCount());
            expectError("ReceiptHandleIsInvalid", () -> s.client.delete(url, first.receiptHandle()));
            s.client.changeVisibility(url, second.receiptHandle(), 0);             // give it back now
            SqsClient.Message third = s.client.receive(url, 1, 0).get(0);
            assertEquals(3, third.receiveCount());
            // a per-receive VisibilityTimeout overrides the queue's
            s.client.changeVisibility(url, third.receiptHandle(), 0);
            s.client.call("ReceiveMessage", Map.of("QueueUrl", url, "VisibilityTimeout", 5));
            s.clock.advance(5_000);
            assertEquals(1, s.client.receive(url, 1, 0).size());
        }
    }

    @Test
    void delaysPerMessageAndPerQueue() throws Exception {
        try (Local s = Local.manualTime()) {
            String url = s.client.createQueue("q", Map.of("DelaySeconds", "10"));
            s.client.send(url, "queue default");
            s.client.sendDelayed(url, "own delay", 2);
            assertEquals(0, s.client.receive(url, 10, 0).size());
            s.clock.advance(2_000);
            assertEquals("own delay", s.client.receive(url, 10, 0).get(0).body());
            s.clock.advance(8_000);
            assertEquals("queue default", s.client.receive(url, 10, 0).get(0).body());
        }
    }

    @Test
    void aLongPollIsWokenBySendInsteadOfWaitingItOut() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("q", Map.of());
            ExecutorService pool = Executors.newSingleThreadExecutor();
            long t0 = System.nanoTime();
            Future<List<SqsClient.Message>> waiting = pool.submit(() -> s.client.receive(url, 1, 10));
            Thread.sleep(300);
            s.client.send(url, "wake up");
            List<SqsClient.Message> got = waiting.get(5, TimeUnit.SECONDS);
            double ms = (System.nanoTime() - t0) / 1e6;
            assertEquals("wake up", got.get(0).body());
            assertTrue(ms < 2_000, "long poll returned after " + ms + " ms, not when the message arrived");
            long t1 = System.nanoTime();
            assertEquals(0, s.client.receive(url, 1, 1).size());                  // nothing comes: waits the full second
            assertTrue((System.nanoTime() - t1) / 1e6 >= 900, "an empty long poll returned early");
            pool.shutdown();
        }
    }

    @Test
    void waitingReceivesAreCappedSoTheyCannotUseUpTheServer() throws Exception {
        try (Local s = new Local(null, 2)) {
            String url = s.client.createQueue("q", Map.of());
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<Future<?>> parked = new ArrayList<>();
            for (int i = 0; i < 2; i++) parked.add(pool.submit(() -> s.client.receive(url, 1, 3)));
            Thread.sleep(300);                                                     // both permits taken
            long t0 = System.nanoTime();
            assertEquals(0, s.client.receive(url, 1, 3).size());
            double ms = (System.nanoTime() - t0) / 1e6;
            assertTrue(ms < 1_000, "the third waiting receive should have been answered at once, took " + ms + " ms");
            for (Future<?> f : parked) f.get(10, TimeUnit.SECONDS);
            pool.shutdown();
        }
    }

    @Test
    void fifoOrderDeduplicationAndRules() throws Exception {
        try (Local s = Local.manualTime()) {
            String url = s.client.createQueue("events.fifo", Map.of());
            for (int i = 0; i < 4; i++) s.client.sendFifo(url, "a" + i, "A", "a" + i);
            String dup = s.client.sendFifo(url, "a0 again", "A", "a0");
            s.client.sendFifo(url, "b0", "B", "b0");
            assertEquals(s.client.receive(url, 10, 0).get(0).messageId(), dup);    // duplicate id: the original's id
            s.clock.advance(30_000);
            List<String> order = new ArrayList<>();
            while (true) {
                List<SqsClient.Message> got = s.client.receive(url, 10, 0);
                if (got.isEmpty()) break;
                for (SqsClient.Message m : got) {
                    if (m.groupId().equals("A")) order.add(m.body());
                    s.client.delete(url, m.receiptHandle());
                }
            }
            assertEquals(List.of("a0", "a1", "a2", "a3"), order);
            expectError("MissingParameter", () -> s.client.send(url, "no group"));
            expectError("InvalidParameterValue", () -> s.client.sendFifo(url, "no dedup id", "A", null));
            String cbd = s.client.createQueue("cbd.fifo", Map.of("ContentBasedDeduplication", "true"));
            assertEquals(s.client.sendFifo(cbd, "same body", "G", null), s.client.sendFifo(cbd, "same body", "G", null));
            String std = s.client.createQueue("std", Map.of());
            expectError("InvalidParameterValue", () -> s.client.sendFifo(std, "x", "G", "d"));
        }
    }

    @Test
    void poisonMessagesGoToTheDeadLetterQueueAndCanBeRedriven() throws Exception {
        try (Local s = Local.manualTime()) {
            String dlq = s.client.createQueue("work-dlq", Map.of());
            String dlqArn = s.client.attributes(dlq).get("QueueArn");
            String work = s.client.createQueue("work", Map.of("VisibilityTimeout", "10",
                    "RedrivePolicy", "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"3\"}"));
            s.client.send(work, "poison");
            s.client.send(work, "fine");
            for (int round = 0; round < 3; round++) {
                for (SqsClient.Message m : s.client.receive(work, 10, 0)) {
                    if (m.body().equals("fine")) s.client.delete(work, m.receiptHandle());
                }
                s.clock.advance(10_000);
            }
            assertEquals(0, s.client.receive(work, 10, 0).size());                // 4th receive moved it out
            assertEquals("poison", s.client.receive(dlq, 10, 0).get(0).body());
            Map<String, Object> sources = s.client.call("ListDeadLetterSourceQueues", Map.of("QueueUrl", dlq));
            assertEquals(List.of(work), sources.get("queueUrls"));
            s.client.redrive(dlqArn);
            SqsClient.Message back = s.client.receive(work, 10, 0).get(0);
            assertEquals("poison", back.body());
            assertEquals(1, back.receiveCount());                                  // a fresh start in the source queue
        }
    }

    @Test
    void batchesSucceedOrFailPerEntry() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("q", Map.of());
            List<Map<String, Object>> entries = new ArrayList<>();
            for (int i = 0; i < 10; i++) entries.add(Map.of("Id", "m" + i, "MessageBody", "body " + i));
            Map<String, Object> res = s.client.call("SendMessageBatch", Map.of("QueueUrl", url, "Entries", entries));
            assertEquals(10, ((List<?>) res.get("Successful")).size());
            List<SqsClient.Message> got = new ArrayList<>();
            while (got.size() < 10) got.addAll(s.client.receive(url, 10, 1));
            List<Map<String, Object>> dels = new ArrayList<>();
            for (int i = 0; i < 9; i++) dels.add(Map.of("Id", "d" + i, "ReceiptHandle", got.get(i).receiptHandle()));
            dels.add(Map.of("Id", "bad", "ReceiptHandle", "nope"));
            res = s.client.call("DeleteMessageBatch", Map.of("QueueUrl", url, "Entries", dels));
            assertEquals(9, ((List<?>) res.get("Successful")).size());
            Map<?, ?> failed = (Map<?, ?>) ((List<?>) res.get("Failed")).get(0);
            assertEquals("bad", failed.get("Id"));
            assertEquals("ReceiptHandleIsInvalid", failed.get("Code"));
            res = s.client.call("ChangeMessageVisibilityBatch", Map.of("QueueUrl", url, "Entries",
                    List.of(Map.of("Id", "v", "ReceiptHandle", got.get(9).receiptHandle(), "VisibilityTimeout", 0))));
            assertEquals(1, ((List<?>) res.get("Successful")).size());
            assertEquals(1, s.client.receive(url, 10, 0).size());                  // visible again at once
            expectError("EmptyBatchRequest", () -> s.client.call("SendMessageBatch", Map.of("QueueUrl", url, "Entries", List.of())));
            List<Map<String, Object>> eleven = new ArrayList<>(entries);
            eleven.add(Map.of("Id", "m10", "MessageBody", "x"));
            expectError("TooManyEntriesInBatchRequest", () -> s.client.call("SendMessageBatch", Map.of("QueueUrl", url, "Entries", eleven)));
            expectError("BatchEntryIdsNotDistinct", () -> s.client.call("SendMessageBatch", Map.of("QueueUrl", url, "Entries",
                    List.of(Map.of("Id", "a", "MessageBody", "1"), Map.of("Id", "a", "MessageBody", "2")))));
            expectError("InvalidBatchEntryId", () -> s.client.call("SendMessageBatch", Map.of("QueueUrl", url, "Entries",
                    List.of(Map.of("Id", "not valid!", "MessageBody", "1")))));
            String big = "x".repeat(200_000);
            expectError("InvalidParameterValue", () -> s.client.call("SendMessageBatch", Map.of("QueueUrl", url, "Entries",
                    List.of(Map.of("Id", "a", "MessageBody", big), Map.of("Id", "b", "MessageBody", big)))));
        }
    }

    @Test
    void badRequestsGetSqsErrorsNotCrashes() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("q", Map.of());
            expectError("UnknownOperationException", () -> s.client.call("LaunchRockets", Map.of()));
            expectError("UnsupportedOperation", () -> s.client.call("PurgeQueue", Map.of("QueueUrl", url)));
            expectError("UnsupportedOperation", () -> s.client.call("SendMessage", Map.of("QueueUrl", url, "MessageBody", "x",
                    "MessageAttributes", Map.of("k", Map.of("DataType", "String", "StringValue", "v")))));
            expectError("MissingParameter", () -> s.client.call("SendMessage", Map.of("QueueUrl", url)));
            expectError("InvalidParameterValue", () -> s.client.send(url, "bad \u0000 character"));
            expectError("InvalidParameterValue", () -> s.client.call("ReceiveMessage", Map.of("QueueUrl", url, "MaxNumberOfMessages", 11)));
            expectError("InvalidParameterValue", () -> s.client.call("ReceiveMessage", Map.of("QueueUrl", url, "WaitTimeSeconds", 21)));
            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(s.endpoint))
                    .header("X-Amz-Target", "AmazonSQS.SendMessage").POST(HttpRequest.BodyPublishers.ofString("{not json")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(400, r.statusCode());
            assertTrue(r.body().contains("InvalidParameterValue"), r.body());
            assertEquals("InvalidParameterValue;Sender", r.headers().firstValue("x-amzn-query-error").orElse(""));
            r = http.send(HttpRequest.newBuilder(URI.create(s.endpoint)).header("X-Amz-Target", "AmazonSQS.SendMessage")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"MessageBody\":\"" + "x".repeat(1_100_000) + "\"}")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(413, r.statusCode());
            r = http.send(HttpRequest.newBuilder(URI.create(s.endpoint)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(400, r.statusCode());
            assertEquals(1, s.client.listQueues(null).size());                    // and the server is still fine
        }
    }

    @Test
    void manyProducersAndConsumersOverHttpLoseNothing() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("q", Map.of());
            int producers = 4, perProducer = 300, consumers = 8;
            Set<String> received = ConcurrentHashMap.newKeySet();
            AtomicInteger duplicates = new AtomicInteger();
            ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
            List<Future<?>> fs = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                int id = p;
                fs.add(pool.submit(() -> {
                    for (int i = 0; i < perProducer; i++) s.client.send(url, id + ":" + i);
                    return null;
                }));
            }
            for (int c = 0; c < consumers; c++) {
                fs.add(pool.submit(() -> {
                    while (received.size() < producers * perProducer) {
                        for (SqsClient.Message m : s.client.receive(url, 10, 1)) {
                            if (!received.add(m.body())) duplicates.incrementAndGet();
                            s.client.delete(url, m.receiptHandle());
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : fs) f.get(120, TimeUnit.SECONDS);
            pool.shutdown();
            assertEquals(producers * perProducer, received.size());
            assertEquals(0, duplicates.get());                                    // 30 s visibility: no redelivery expected
            Map<String, String> a = s.client.attributes(url);
            assertEquals("0", a.get("ApproximateNumberOfMessages"));
            assertEquals("0", a.get("ApproximateNumberOfMessagesNotVisible"));
        }
    }

    @Test
    void responsesAreNotHeldBackByNagleAndDelayedAck() throws Exception {
        try (Local s = Local.realTime()) {
            String url = s.client.createQueue("q", Map.of());
            for (int i = 0; i < 20; i++) s.client.attributes(url);
            long t0 = System.nanoTime();
            for (int i = 0; i < 100; i++) s.client.attributes(url);
            double meanMs = (System.nanoTime() - t0) / 1e6 / 100;
            assertTrue(meanMs < 20, "mean latency " + meanMs + " ms");
        }
    }
}
