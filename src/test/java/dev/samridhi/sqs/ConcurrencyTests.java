package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.testkit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** Real threads, real clock. */
public class ConcurrencyTests {
    @Test
    void manyProducersAndConsumersDeleteEveryMessageExactlyOnce() throws Exception {
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(60_000), Clock.SYSTEM);
        int producers = 8, perProducer = 2_500, total = producers * perProducer;
        Map<String, AtomicInteger> deletedCount = new ConcurrentHashMap<>();
        AtomicInteger deletedTotal = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            final int pid = p;
            threads.add(new Thread(() -> {
                for (int i = 0; i < perProducer; i++) q.send(pid + ":" + i);
            }));
        }
        for (int c = 0; c < 8; c++) {
            threads.add(new Thread(() -> {
                while (deletedTotal.get() < total) {
                    for (ReceivedMessage m : q.receive(10, 20)) {
                        if (q.delete(m.receiptHandle())) {
                            deletedCount.computeIfAbsent(m.body(), k -> new AtomicInteger()).incrementAndGet();
                            deletedTotal.incrementAndGet();
                        }
                    }
                }
            }));
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join(60_000);
        assertEquals(total, deletedCount.size());                          // none lost
        for (AtomicInteger n : deletedCount.values()) assertEquals(1, n.get());   // none deleted twice
        assertEquals(0, q.stats().total());
    }

    @Test
    void fifoGroupsStayInOrderWithManyCompetingConsumers() throws Exception {
        MessageQueue q = new MessageQueue("q.fifo", QueueConfig.fifo(60_000), Clock.SYSTEM);
        int groups = 20, perGroup = 200;
        for (int i = 0; i < perGroup; i++)
            for (int g = 0; g < groups; g++) q.send(g + ":" + i, 0, "g" + g, g + "-" + i);
        Map<String, List<Integer>> processed = new ConcurrentHashMap<>();
        AtomicInteger done = new AtomicInteger();
        List<Thread> consumers = new ArrayList<>();
        for (int c = 0; c < 8; c++) {
            consumers.add(new Thread(() -> {
                while (done.get() < groups * perGroup) {
                    for (ReceivedMessage m : q.receive(10, 20)) {
                        String[] parts = m.body().split(":");
                        processed.computeIfAbsent(parts[0], k -> new CopyOnWriteArrayList<>()).add(Integer.parseInt(parts[1]));
                        q.delete(m.receiptHandle());
                        done.incrementAndGet();
                    }
                }
            }));
        }
        for (Thread t : consumers) t.start();
        for (Thread t : consumers) t.join(60_000);
        assertEquals(groups, processed.size());
        for (List<Integer> seen : processed.values()) {
            assertEquals(perGroup, seen.size());
            for (int i = 0; i < perGroup; i++) assertEquals(i, seen.get(i));   // processed strictly in order
        }
    }

    @Test
    void longPollingWakesAsSoonAsAMessageArrives() throws Exception {
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(30_000), Clock.SYSTEM);
        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                return;
            }
            q.send("ping");
        });
        long start = System.nanoTime();
        producer.start();
        List<ReceivedMessage> got = q.receive(1, 10_000);
        long tookMs = (System.nanoTime() - start) / 1_000_000;
        producer.join();
        assertEquals(1, got.size());
        assertTrue(tookMs < 5_000, "returned when the message arrived, not after the full 10 s wait (took " + tookMs + " ms)");
        long emptyStart = System.nanoTime();
        assertTrue(q.receive(1, 200).isEmpty(), "empty queue: returns empty after the wait");
        assertTrue((System.nanoTime() - emptyStart) / 1_000_000 >= 150, "actually waited");
    }
}
