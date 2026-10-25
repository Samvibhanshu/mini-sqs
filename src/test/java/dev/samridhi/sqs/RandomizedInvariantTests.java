package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.testkit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Thousands of random operation sequences (send, receive, delete with fresh or stale handles, change visibility,
 * let time pass) with a manual clock, checking the properties a queue must never break:
 * <ol>
 *   <li>Nothing is lost or invented: every message ever accepted is in the queue, in the DLQ, deleted by us, or
 *       expired, and never in two of those places.</li>
 *   <li>A message is never delivered while another delivery of it is still within its visibility timeout.</li>
 *   <li>Receive counts go up by exactly one per delivery.</li>
 *   <li>FIFO: within a group, first deliveries happen in send order, and nothing later in a group is delivered while
 *       an earlier message of the group is still in the queue.</li>
 * </ol>
 */
public class RandomizedInvariantTests {
    @Test
    void standardQueueKeepsItsPromises() {
        for (int seed = 0; seed < 300; seed++) run(seed, false);
    }

    @Test
    void fifoQueueKeepsItsPromises() {
        for (int seed = 0; seed < 300; seed++) run(seed, true);
    }

    private static void run(int seed, boolean fifo) {
        Random rnd = new Random(seed);
        Clock.Manual clock = new Clock.Manual(0);
        long vt = 1_000 + rnd.nextInt(5_000);
        long retention = 50_000 + rnd.nextInt(100_000);
        MessageQueue dlq = new MessageQueue("dlq", (fifo ? QueueConfig.fifo(10_000) : QueueConfig.standard(10_000)).withRetention(10_000_000), clock);
        QueueConfig base = (fifo ? QueueConfig.fifo(vt) : QueueConfig.standard(vt)).withRetention(retention);
        MessageQueue q = new MessageQueue("q", base.withDeadLetterQueue(dlq, 1 + rnd.nextInt(4)), clock);

        Map<String, Long> sentAt = new HashMap<>();
        Map<String, String> groupOf = new HashMap<>();
        Map<String, Integer> seqInGroup = new HashMap<>();
        Map<String, Integer> nextSeq = new HashMap<>();
        Map<String, Long> hiddenUntil = new HashMap<>();
        Map<String, Integer> lastCount = new HashMap<>();
        Set<String> deleted = new HashSet<>();
        Set<String> everDelivered = new HashSet<>();
        Map<String, Integer> lastFirstDeliveredSeq = new HashMap<>();
        List<ReceivedMessage> handles = new ArrayList<>();

        for (int step = 0; step < 300; step++) {
            int op = rnd.nextInt(10);
            long now = clock.nowMillis();
            if (op < 3) {
                String g = fifo ? "g" + rnd.nextInt(3) : null;
                String id = q.send("b" + step, fifo ? 0 : (rnd.nextInt(4) == 0 ? rnd.nextInt(3_000) : 0), g, fifo ? "d" + step : null);
                sentAt.put(id, now);
                if (fifo) {
                    groupOf.put(id, g);
                    int s = nextSeq.merge(g, 1, Integer::sum);
                    seqInGroup.put(id, s);
                }
            } else if (op < 6) {
                for (ReceivedMessage m : q.receive(1 + rnd.nextInt(10))) {
                    Long hidden = hiddenUntil.get(m.messageId());
                    assertTrue(hidden == null || hidden <= now, "delivered while still in flight: " + m.messageId());
                    assertTrue(!deleted.contains(m.messageId()), "delivered after delete");
                    assertEquals(lastCount.getOrDefault(m.messageId(), 0) + 1, m.receiveCount());
                    lastCount.put(m.messageId(), m.receiveCount());
                    hiddenUntil.put(m.messageId(), now + vt);
                    if (fifo) {
                        String g = groupOf.get(m.messageId());
                        int s = seqInGroup.get(m.messageId());
                        // every earlier message of this group must already be gone (deleted, dead-lettered or expired)
                        for (String other : q.messageIds()) {
                            if (g.equals(groupOf.get(other)) && seqInGroup.get(other) < s) {
                                throw new AssertionError("seed " + seed + ": delivered " + s + " while earlier " + seqInGroup.get(other) + " of " + g + " is still queued");
                            }
                        }
                        if (everDelivered.add(m.messageId())) {
                            assertTrue(s > lastFirstDeliveredSeq.getOrDefault(g, 0), "group order broken");
                            lastFirstDeliveredSeq.put(g, s);
                        }
                    }
                    handles.add(m);
                }
            } else if (op < 8 && !handles.isEmpty()) {
                ReceivedMessage m = handles.remove(rnd.nextInt(handles.size()));
                boolean current = q.messageIds().contains(m.messageId()) && lastCount.get(m.messageId()) == m.receiveCount();
                boolean ok = q.delete(m.receiptHandle());
                assertEquals(current, ok);                                     // stale handles must fail, fresh ones succeed
                if (ok) {
                    deleted.add(m.messageId());
                    hiddenUntil.remove(m.messageId());
                }
            } else if (op == 8 && !handles.isEmpty()) {
                ReceivedMessage m = handles.get(rnd.nextInt(handles.size()));
                long t = rnd.nextInt(3) == 0 ? 0 : rnd.nextInt(5_000);
                if (q.changeVisibility(m.receiptHandle(), t)) hiddenUntil.put(m.messageId(), now + t);
            } else {
                clock.advance(rnd.nextInt(4_000));
            }
            // conservation: each accepted message is in exactly one place, or deleted, or expired
            Set<String> inQ = q.messageIds(), inDlq = dlq.messageIds();
            for (String id : sentAt.keySet()) {
                int places = (inQ.contains(id) ? 1 : 0) + (inDlq.contains(id) ? 1 : 0) + (deleted.contains(id) ? 1 : 0);
                boolean mayHaveExpired = clock.nowMillis() - sentAt.get(id) >= retention;
                assertTrue(places == 1 || (places == 0 && mayHaveExpired), "seed " + seed + ": message " + id + " is in " + places + " places");
            }
            for (String id : inQ) assertTrue(sentAt.containsKey(id), "invented message");
        }
    }
}
