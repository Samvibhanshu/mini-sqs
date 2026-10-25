package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.testkit.Test;
import java.util.List;

public class DeadLetterTests {
    private final Clock.Manual clock = new Clock.Manual(0);

    @Test
    void aPoisonMessageMovesToTheDlqAfterMaxReceives() {
        MessageQueue dlq = new MessageQueue("dlq", QueueConfig.standard(30_000), clock);
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(1_000).withDeadLetterQueue(dlq, 3), clock);
        String id = q.send("bad payload");
        for (int attempt = 1; attempt <= 3; attempt++) {
            List<ReceivedMessage> got = q.receive(1);
            assertEquals(1, got.size());
            assertEquals(attempt, got.get(0).receiveCount());
            clock.advance(1_000);                                           // handler failed; never deleted
        }
        assertTrue(q.receive(1).isEmpty(), "4th receive moves it instead of delivering it");
        assertEquals(0, q.stats().total());
        ReceivedMessage dead = dlq.receive(1).get(0);
        assertEquals(id, dead.messageId());                                 // same id, so it can be traced
        assertEquals(1, dead.receiveCount());
    }

    @Test
    void healthyMessagesAreNotHeldUpByAPoisonOne() {
        MessageQueue dlq = new MessageQueue("dlq", QueueConfig.standard(30_000), clock);
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(1_000).withDeadLetterQueue(dlq, 1), clock);
        q.send("poison");
        q.receive(1);
        clock.advance(1_000);
        q.send("fine");
        List<ReceivedMessage> got = q.receive(10);                          // poison dead-lettered in the same call
        assertEquals(1, got.size());
        assertEquals("fine", got.get(0).body());
        assertEquals(1, dlq.stats().total());
    }

    @Test
    void redriveMovesMessagesBackAfterTheFix() {
        MessageQueue dlq = new MessageQueue("dlq", QueueConfig.standard(30_000), clock);
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(1_000).withDeadLetterQueue(dlq, 1), clock);
        for (int i = 0; i < 5; i++) q.send("m" + i);
        q.receive(10);
        clock.advance(1_000);
        q.receive(10);                                                       // all five dead-lettered
        assertEquals(5, dlq.stats().total());
        assertEquals(5, dlq.redriveTo(q));
        assertEquals(0, dlq.stats().total());
        assertEquals(5, q.receive(10).size());
    }

    @Test
    void retentionCountsFromTheOriginalSendEvenAfterMovingToTheDlq() {
        MessageQueue dlq = new MessageQueue("dlq", QueueConfig.standard(1_000).withRetention(10_000), clock);
        MessageQueue q = new MessageQueue("q", QueueConfig.standard(1_000).withRetention(10_000).withDeadLetterQueue(dlq, 1), clock);
        q.send("m");
        q.receive(1);
        clock.advance(8_000);
        q.receive(1);                                                        // moved to DLQ at t = 8 s
        assertEquals(1, dlq.stats().total());
        clock.advance(2_000);                                                // t = 10 s since the ORIGINAL send
        assertTrue(dlq.receive(1).isEmpty(), "expired: moving does not reset its age");
    }

    @Test
    void fifoDeadLetteringUnblocksTheRestOfTheGroupInOrder() {
        MessageQueue dlq = new MessageQueue("dlq.fifo", QueueConfig.fifo(30_000), clock);
        MessageQueue q = new MessageQueue("q.fifo", QueueConfig.fifo(1_000).withDeadLetterQueue(dlq, 2), clock);
        q.send("1", 0, "g", "1");
        q.send("2", 0, "g", "2");
        q.receive(10);
        clock.advance(1_000);
        q.receive(10);
        clock.advance(1_000);
        assertEquals("2", q.receive(10).get(0).body());                    // "1" dead-lettered, "2" is next
        assertEquals("1", dlq.receive(10).get(0).body());
    }
}
