package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.testkit.Test;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerTests {
    @Test
    void backoffDoublesAndIsCapped() {
        long[] expected = {100, 100, 200, 400, 800, 1600, 3000, 3000};
        for (int n = 0; n < expected.length; n++) assertEquals(expected[n], Worker.backoffMs(n, 100, 3000));
        assertEquals(3000L, Worker.backoffMs(Integer.MAX_VALUE, 100, 3000));    // no overflow for huge counts
    }

    private static void waitFor(java.util.function.BooleanSupplier cond, long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(10);
    }

    @Test
    void successfulMessagesAreDeletedAndTransientFailuresRetried() throws Exception {
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(30_000), Clock.SYSTEM);
        Set<String> done = ConcurrentHashMap.newKeySet();
        Set<String> failedOnce = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 200; i++) q.send("job-" + i);
        try (Worker w = new Worker(q, m -> {
            if (m.body().endsWith("7") && failedOnce.add(m.body())) throw new IllegalStateException("flaky downstream");
            done.add(m.body());
        }, 4, 20, 200)) {
            waitFor(() -> done.size() == 200, 20_000);
            assertEquals(200, done.size());
            assertEquals(20L, w.failed());                                  // job-7, job-17, ... failed once each
        }
        assertEquals(0, q.stats().total());
    }

    @Test
    void aMessageThatAlwaysFailsEndsUpInTheDlq() throws Exception {
        MessageQueue dlq = new MessageQueue("dlq", QueueConfig.standard(30_000), Clock.SYSTEM);
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(30_000).withDeadLetterQueue(dlq, 3), Clock.SYSTEM);
        AtomicInteger attempts = new AtomicInteger();
        q.send("poison");
        q.send("ok");
        try (Worker w = new Worker(q, m -> {
            if (m.body().equals("poison")) {
                attempts.incrementAndGet();
                throw new IllegalArgumentException("cannot parse");
            }
        }, 2, 10, 50)) {
            waitFor(() -> dlq.stats().total() == 1, 20_000);
            assertEquals(1L, w.succeeded());
        }
        assertEquals(3, attempts.get());                                    // tried maxReceiveCount times, no more
        assertEquals("poison", dlq.receive(1).get(0).body());
        assertEquals(0, q.stats().total());
    }
}
