package dev.samridhi.sqs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A pool of consumer threads: receive, handle, delete on success. On failure the message is NOT deleted; its
 * visibility is set to an exponentially growing delay (1x, 2x, 4x ... capped), so a struggling downstream gets
 * breathing room instead of a retry storm. After {@code maxReceiveCount} failures the queue sends it to the DLQ.
 */
public final class Worker implements AutoCloseable {
    @FunctionalInterface
    public interface Handler {
        void handle(ReceivedMessage message) throws Exception;
    }

    private final MessageQueue queue;
    private final Handler handler;
    private final long baseBackoffMs, maxBackoffMs;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong succeeded = new AtomicLong(), failed = new AtomicLong();
    private final List<Thread> threads = new ArrayList<>();

    public Worker(MessageQueue queue, Handler handler, int threadCount, long baseBackoffMs, long maxBackoffMs) {
        if (threadCount < 1) throw new IllegalArgumentException("threads >= 1");
        this.queue = queue;
        this.handler = handler;
        this.baseBackoffMs = baseBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(this::loop, queue.name() + "-worker-" + i);
            t.setDaemon(true);
            threads.add(t);
            t.start();
        }
    }

    /** Delay before the next attempt after the n-th failed delivery: base * 2^(n-1), capped at max. */
    public static long backoffMs(int receiveCount, long base, long max) {
        int shift = Math.min(Math.max(receiveCount - 1, 0), 30);              // cap the shift: 1L << 63 would overflow
        return Math.min(max, base * (1L << shift));
    }

    private void loop() {
        while (running.get()) {
            for (ReceivedMessage m : queue.receive(10, 50)) {
                try {
                    handler.handle(m);
                    queue.delete(m.receiptHandle());
                    succeeded.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                    queue.changeVisibility(m.receiptHandle(), backoffMs(m.receiveCount(), baseBackoffMs, maxBackoffMs));
                }
            }
        }
    }

    public long succeeded() {
        return succeeded.get();
    }

    public long failed() {
        return failed.get();
    }

    @Override
    public void close() {
        running.set(false);
        try {
            for (Thread t : threads) t.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
