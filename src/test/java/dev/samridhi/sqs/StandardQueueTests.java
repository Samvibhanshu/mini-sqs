package dev.samridhi.sqs;

import static dev.samridhi.sqs.testkit.Assert.assertEquals;
import static dev.samridhi.sqs.testkit.Assert.assertFalse;
import static dev.samridhi.sqs.testkit.Assert.assertThrows;
import static dev.samridhi.sqs.testkit.Assert.assertTrue;

import dev.samridhi.sqs.testkit.Test;
import java.util.List;

public class StandardQueueTests {
    private final Clock.Manual clock = new Clock.Manual(1_000_000);
    private final MessageQueue q = new MessageQueue("q", QueueConfig.standard(30_000), clock);

    @Test
    void aReceivedMessageIsHiddenUntilTheVisibilityTimeoutThenComesBack() {
        String id = q.send("hello");
        ReceivedMessage first = q.receive(1).get(0);
        assertEquals(id, first.messageId());
        assertEquals(1, first.receiveCount());
        assertTrue(q.receive(10).isEmpty(), "in flight: nobody else may see it");
        clock.advance(29_999);
        assertTrue(q.receive(10).isEmpty(), "still hidden 1 ms before the timeout");
        clock.advance(1);
        ReceivedMessage second = q.receive(1).get(0);                     // consumer 1 "crashed": redelivered
        assertEquals(id, second.messageId());
        assertEquals(2, second.receiveCount());
        assertFalse(first.receiptHandle().equals(second.receiptHandle()), "each delivery gets a new handle");
    }

    @Test
    void onlyTheLatestReceiptHandleCanDelete() {
        q.send("m");
        ReceivedMessage first = q.receive(1).get(0);
        clock.advance(30_000);
        ReceivedMessage second = q.receive(1).get(0);
        assertFalse(q.delete(first.receiptHandle()), "a slow consumer must not delete a message someone else now owns");
        assertTrue(q.delete(second.receiptHandle()));
        assertFalse(q.delete(second.receiptHandle()), "deleting twice is a no-op");
        clock.advance(60_000);
        assertTrue(q.receive(10).isEmpty(), "deleted for good");
        assertEquals(0, q.stats().total());
    }

    @Test
    void changeVisibilityExtendsOrReleasesImmediately() {
        q.send("long job");
        ReceivedMessage m = q.receive(1).get(0);
        clock.advance(20_000);
        assertTrue(q.changeVisibility(m.receiptHandle(), 60_000));       // "still working on it"
        clock.advance(30_000);
        assertTrue(q.receive(1).isEmpty(), "extension kept it hidden past the original 30 s");
        assertTrue(q.changeVisibility(m.receiptHandle(), 0));            // "give up, let someone else try"
        assertEquals(1, q.receive(1).size());
        assertFalse(q.changeVisibility("no-such-handle", 10));
    }

    @Test
    void delayedMessagesAreInvisibleUntilTheirDelayEnds() {
        q.send("later", 10_000, null, null);
        q.send("now");
        assertEquals(new QueueStats(1, 0, 1), q.stats());
        assertEquals("now", q.receive(10).get(0).body());
        clock.advance(10_000);
        assertEquals("later", q.receive(10).get(0).body());
    }

    @Test
    void receiveReturnsAtMostTheRequestedNumberInVisibilityOrder() {
        for (int i = 0; i < 25; i++) q.send("m" + i);
        List<ReceivedMessage> batch = q.receive(10);
        assertEquals(10, batch.size());
        for (int i = 0; i < 10; i++) assertEquals("m" + i, batch.get(i).body());
        assertEquals(new QueueStats(15, 10, 0), q.stats());
        assertThrows(IllegalArgumentException.class, () -> q.receive(11));
        assertThrows(IllegalArgumentException.class, () -> q.receive(0));
    }

    @Test
    void messagesExpireAfterTheRetentionPeriodEvenIfNeverReceived() {
        MessageQueue shortLived = new MessageQueue("s", QueueConfig.standard(1_000).withRetention(60_000), clock);
        shortLived.send("old");
        clock.advance(59_999);
        shortLived.send("new");
        clock.advance(1);
        List<ReceivedMessage> got = shortLived.receive(10);
        assertEquals(1, got.size());
        assertEquals("new", got.get(0).body());
        assertEquals(1, shortLived.messageIds().size());
    }

    @Test
    void inputValidation() {
        assertThrows(IllegalArgumentException.class, () -> q.send(""));
        assertThrows(IllegalArgumentException.class, () -> q.send("x".repeat(QueueConfig.MAX_BODY_BYTES + 1)));
        assertThrows(IllegalArgumentException.class, () -> q.send("x", -1, null, null));
        assertThrows(IllegalArgumentException.class, () -> q.send("x", QueueConfig.MAX_DELAY_MS + 1, null, null));
        assertThrows(IllegalArgumentException.class, () -> q.send("x", 0, "group", null));          // FIFO-only option
        assertThrows(IllegalArgumentException.class, () -> new QueueConfig(1000, 3, null, 1000, false, 0));
        q.send("é".repeat(QueueConfig.MAX_BODY_BYTES / 2));                // limit is in UTF-8 bytes, not characters
        assertThrows(IllegalArgumentException.class, () -> q.send("é".repeat(QueueConfig.MAX_BODY_BYTES / 2 + 1)));
    }
}
