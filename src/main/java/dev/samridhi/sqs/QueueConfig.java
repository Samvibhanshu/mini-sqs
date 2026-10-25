package dev.samridhi.sqs;

/**
 * Queue settings, mirroring the SQS ones that matter.
 *
 * @param visibilityTimeoutMs how long a received message stays hidden from other consumers (SQS default 30 s)
 * @param maxReceiveCount     deliveries allowed before the message goes to {@code deadLetterQueue}; 0 = never
 * @param deadLetterQueue     where poison messages go (must be FIFO if this queue is FIFO), or null
 * @param retentionMs         messages older than this (since first sent) are deleted unread (SQS default 4 days)
 * @param fifo                strict order per message group, one in-flight message per group, deduplication
 * @param dedupWindowMs       FIFO only: a repeated deduplication id within this window is ignored (SQS: 5 minutes)
 */
public record QueueConfig(long visibilityTimeoutMs, int maxReceiveCount, MessageQueue deadLetterQueue,
                          long retentionMs, boolean fifo, long dedupWindowMs) {
    public static final int MAX_BODY_BYTES = 256 * 1024;
    public static final long MAX_DELAY_MS = 15 * 60_000L;

    public QueueConfig {
        if (visibilityTimeoutMs < 0 || visibilityTimeoutMs > 12 * 3_600_000L) throw new IllegalArgumentException("visibility timeout 0..12h");
        if (maxReceiveCount < 0) throw new IllegalArgumentException("maxReceiveCount >= 0");
        if ((maxReceiveCount > 0) != (deadLetterQueue != null)) throw new IllegalArgumentException("maxReceiveCount and deadLetterQueue go together");
        if (deadLetterQueue != null && deadLetterQueue.config().fifo() != fifo) throw new IllegalArgumentException("DLQ must be the same type (FIFO or standard)");
        if (retentionMs <= 0) throw new IllegalArgumentException("retention must be positive");
        if (dedupWindowMs < 0) throw new IllegalArgumentException("dedup window >= 0");
    }

    public static QueueConfig standard(long visibilityTimeoutMs) {
        return new QueueConfig(visibilityTimeoutMs, 0, null, 4L * 24 * 3_600_000, false, 0);
    }

    public static QueueConfig fifo(long visibilityTimeoutMs) {
        return new QueueConfig(visibilityTimeoutMs, 0, null, 4L * 24 * 3_600_000, true, 5 * 60_000L);
    }

    public QueueConfig withDeadLetterQueue(MessageQueue dlq, int maxReceives) {
        return new QueueConfig(visibilityTimeoutMs, maxReceives, dlq, retentionMs, fifo, dedupWindowMs);
    }

    public QueueConfig withRetention(long ms) {
        return new QueueConfig(visibilityTimeoutMs, maxReceiveCount, deadLetterQueue, ms, fifo, dedupWindowMs);
    }
}
