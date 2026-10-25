package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertThrows;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.testkit.Test;
import java.util.ArrayList;
import java.util.List;

public class FifoQueueTests {
    private final Clock.Manual clock = new Clock.Manual(0);
    private final MessageQueue q = new MessageQueue("orders.fifo", QueueConfig.fifo(30_000), clock);

    private List<String> bodies(List<ReceivedMessage> ms) {
        List<String> out = new ArrayList<>();
        for (ReceivedMessage m : ms) out.add(m.body());
        return out;
    }

    @Test
    void oneGroupIsDeliveredStrictlyInOrderOneAtATime() {
        for (int i = 1; i <= 3; i++) q.send("order-42 step " + i, 0, "order-42", "d" + i);
        ReceivedMessage a = q.receive(10).get(0);
        assertEquals("order-42 step 1", a.body());
        assertTrue(q.receive(10).isEmpty(), "step 2 must wait until step 1 is done");
        q.delete(a.receiptHandle());
        ReceivedMessage b = q.receive(10).get(0);
        assertEquals("order-42 step 2", b.body());
        clock.advance(30_000);                                              // consumer of step 2 died
        ReceivedMessage again = q.receive(10).get(0);
        assertEquals("order-42 step 2", again.body());                      // retried BEFORE step 3, order kept
        assertEquals(2, again.receiveCount());
    }

    @Test
    void differentGroupsAreProcessedInParallel() {
        q.send("a1", 0, "A", "1");
        q.send("a2", 0, "A", "2");
        q.send("b1", 0, "B", "3");
        q.send("c1", 0, "C", "4");
        assertEquals(List.of("a1", "b1", "c1"), bodies(q.receive(10)));   // one head per group
        assertEquals(new QueueStats(0, 3, 1), q.stats());                   // a2 is waiting behind a1
    }

    @Test
    void duplicatesWithinTheWindowAreDroppedButLaterOnesAreNot() {
        String id1 = q.send("charge card", 0, "g", "pay-1");
        String id2 = q.send("charge card", 0, "g", "pay-1");                // producer retried after a timeout
        assertEquals(id1, id2);
        assertEquals(1, q.messageIds().size());
        clock.advance(5 * 60_000);
        String id3 = q.send("charge card", 0, "g", "pay-1");                // window over: a genuinely new message
        assertTrue(!id3.equals(id1), "outside the window it is a new message");
        assertEquals(2, q.messageIds().size());
    }

    @Test
    void contentBasedDeduplicationUsesTheBody() {
        assertEquals(q.send("same body", 0, "g", null), q.send("same body", 0, "g", null));
        assertTrue(!q.send("other body", 0, "g", null).equals(q.send("same body", 0, "g", null)), "different body, different message");
    }

    @Test
    void fifoNeedsAGroupId() {
        assertThrows(IllegalArgumentException.class, () -> q.send("x", 0, null, "d"));
        assertThrows(IllegalArgumentException.class, () -> new QueueConfig(1000, 0, null, 1000, true, 1000)
                .withDeadLetterQueue(new MessageQueue("std", QueueConfig.standard(1000), clock), 3));
    }

    @Test
    void busyGroupsCannotStarveQuietOnes() {
        Clock.Manual clock = new Clock.Manual(0);
        MessageQueue q = new MessageQueue("q", QueueConfig.fifo(30_000), clock);
        for (int g = 0; g < 20; g++) for (int k = 0; k < 5; k++) q.send("g" + g + "-" + k, 0, "g" + g, "d" + g + "-" + k);
        // A consumer that takes one message at a time and finishes it: every group must get a turn before any repeats.
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (int i = 0; i < 20; i++) {
            ReceivedMessage m = q.receive(1).get(0);
            assertTrue(seen.add(m.groupId()), "group " + m.groupId() + " served twice before others had a turn");
            q.delete(m.receiptHandle());
        }
    }
}
