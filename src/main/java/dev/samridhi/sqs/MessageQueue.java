package dev.samridhi.sqs;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An in-memory message queue with Amazon SQS semantics.
 *
 * <h2>The core idea: messages are hidden, not removed, when received</h2>
 * A consumer that receives a message might crash before finishing. So receiving only hides the message for the
 * visibility timeout; the consumer must {@link #delete} it when done. If it never does, the message reappears and
 * another consumer gets it. That is <b>at-least-once delivery</b>: nothing is lost, but a message can be processed
 * twice, so consumers must be idempotent.
 *
 * <h2>Standard vs FIFO</h2>
 * <ul>
 *   <li><b>Standard:</b> any consumer may get any visible message; order is best-effort (by when it became visible).</li>
 *   <li><b>FIFO:</b> messages carry a group id (e.g. an order id). Within a group, delivery is strictly in send order
 *       and only one message of the group is in flight at a time, so a group's messages are processed one by one.
 *       Different groups proceed in parallel. A deduplication id seen within the window is dropped.</li>
 * </ul>
 *
 * <h2>Dead-letter queue</h2>
 * A "poison" message that fails every time would otherwise circulate forever. After {@code maxReceiveCount}
 * deliveries the next receive moves it to the DLQ instead of delivering it again. {@link #redriveTo} moves DLQ
 * messages back once the bug is fixed.
 *
 * <p>Thread-safe: one lock per queue; receivers can long-poll ({@link #receive(int, long)}) and are woken by sends.
 */
public final class MessageQueue {
    private static final class Msg {
        final String id, body, groupId;
        final long seq, firstSentAt;
        long visibleAt;              // not deliverable before this time (delay, or hidden after a receive)
        int receiveCount;
        String receipt;              // latest receipt handle, null if never received

        Msg(String id, String body, String groupId, long seq, long firstSentAt, long visibleAt) {
            this.id = id;
            this.body = body;
            this.groupId = groupId;
            this.seq = seq;
            this.firstSentAt = firstSentAt;
            this.visibleAt = visibleAt;
        }
    }

    private record Dedup(String messageId, long expiresAt) {}

    private static final AtomicLong SEQ = new AtomicLong();

    private final String name;
    private final QueueConfig config;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    // Standard queue: candidates ordered by when they become visible (ties: send order). O(log n) receive.
    private final TreeSet<Msg> byVisibleAt = new TreeSet<>(Comparator.<Msg>comparingLong(m -> m.visibleAt).thenComparingLong(m -> m.seq));
    // FIFO queue: one deque per message group, in send order. Only the head of a group can ever be delivered.
    private final LinkedHashMap<String, ArrayDeque<Msg>> groups = new LinkedHashMap<>();
    private final Map<String, Dedup> dedup = new HashMap<>();
    private final Map<String, Msg> byReceipt = new HashMap<>();
    private final Map<String, Msg> live = new LinkedHashMap<>();     // every message currently in this queue, by id

    public MessageQueue(String name, QueueConfig config, Clock clock) {
        this.name = name;
        this.config = config;
        this.clock = clock;
    }

    public String name() {
        return name;
    }

    public QueueConfig config() {
        return config;
    }

    // ------------------------------------------------------------------------------------------------ sending

    /** Sends to a standard queue. Returns the message id. */
    public String send(String body) {
        return send(body, 0, null, null);
    }

    /**
     * Sends a message, visible after {@code delayMs}. For a FIFO queue {@code groupId} is required; if
     * {@code dedupId} is null the SHA-256 of the body is used (SQS "content-based deduplication").
     * Returns the message id; for a duplicate inside the window, the id of the original message.
     */
    public String send(String body, long delayMs, String groupId, String dedupId) {
        if (body == null || body.isEmpty()) throw new IllegalArgumentException("body must not be empty");
        if (body.getBytes(StandardCharsets.UTF_8).length > QueueConfig.MAX_BODY_BYTES) throw new IllegalArgumentException("body over 256 KB");
        if (delayMs < 0 || delayMs > QueueConfig.MAX_DELAY_MS) throw new IllegalArgumentException("delay 0..15 min");
        if (config.fifo() && (groupId == null || groupId.isEmpty())) throw new IllegalArgumentException("FIFO messages need a groupId");
        if (!config.fifo() && (groupId != null || dedupId != null)) throw new IllegalArgumentException("groupId/dedupId are FIFO-only");
        lock.lock();
        try {
            long now = clock.nowMillis();
            if (config.fifo()) {
                String key = dedupId != null ? dedupId : sha256(body);
                dedup.values().removeIf(d -> d.expiresAt() <= now);
                Dedup seen = dedup.get(key);
                if (seen != null) return seen.messageId();          // duplicate inside the window: accepted, not stored
                String id = UUID.randomUUID().toString();
                dedup.put(key, new Dedup(id, now + config.dedupWindowMs()));
                enqueue(new Msg(id, body, groupId, SEQ.incrementAndGet(), now, now + delayMs));
                return id;
            }
            String id = UUID.randomUUID().toString();
            enqueue(new Msg(id, body, null, SEQ.incrementAndGet(), now, now + delayMs));
            return id;
        } finally {
            lock.unlock();
        }
    }

    /** Called with the lock held. */
    private void enqueue(Msg m) {
        live.put(m.id, m);
        if (config.fifo()) groups.computeIfAbsent(m.groupId, g -> new ArrayDeque<>()).addLast(m);
        else byVisibleAt.add(m);
        changed.signalAll();
    }

    /** Accepts a message moved from another queue (DLQ move or redrive), keeping its id and original send time. */
    private void adopt(String id, String body, String groupId, long firstSentAt) {
        lock.lock();
        try {
            enqueue(new Msg(id, body, groupId, SEQ.incrementAndGet(), firstSentAt, clock.nowMillis()));
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------------------------------------------------------------------------- receiving

    /** Receives up to {@code max} (1..10) messages without waiting. */
    public List<ReceivedMessage> receive(int max) {
        return receive(max, 0);
    }

    /**
     * Long polling: waits up to {@code waitMs} of real time for at least one message instead of returning empty
     * at once. Cheaper than tight polling loops, and a send wakes the waiter immediately.
     */
    public List<ReceivedMessage> receive(int max, long waitMs) {
        if (max < 1 || max > 10) throw new IllegalArgumentException("max 1..10");
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMs);
        List<Msg> toDeadLetter = new ArrayList<>();
        List<ReceivedMessage> out;
        lock.lock();
        try {
            while (true) {
                out = config.fifo() ? receiveFifo(max, toDeadLetter) : receiveStandard(max, toDeadLetter);
                long remaining = deadline - System.nanoTime();
                if (!out.isEmpty() || remaining <= 0) break;
                long untilNextVisible = TimeUnit.MILLISECONDS.toNanos(Math.max(1, nextVisibleAt() - clock.nowMillis()));
                try {
                    changed.awaitNanos(Math.min(remaining, untilNextVisible));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        // Moved outside our lock, so two queues' locks are never held together (no lock-order deadlock).
        for (Msg m : toDeadLetter) config.deadLetterQueue().adopt(m.id, m.body, m.groupId, m.firstSentAt);
        return out;
    }

    private List<ReceivedMessage> receiveStandard(int max, List<Msg> toDeadLetter) {
        long now = clock.nowMillis();
        List<ReceivedMessage> out = new ArrayList<>();
        while (out.size() < max && !byVisibleAt.isEmpty() && byVisibleAt.first().visibleAt <= now) {
            Msg m = byVisibleAt.pollFirst();
            if (expired(m, now) || deadLettered(m, toDeadLetter)) continue;  // already removed from every index
            out.add(deliver(m, now));
            byVisibleAt.add(m);                                            // re-indexed under its new visibleAt
        }
        return out;
    }

    private List<ReceivedMessage> receiveFifo(int max, List<Msg> toDeadLetter) {
        long now = clock.nowMillis();
        List<ReceivedMessage> out = new ArrayList<>();
        List<String> served = new ArrayList<>();
        Iterator<Map.Entry<String, ArrayDeque<Msg>>> it = groups.entrySet().iterator();
        while (it.hasNext() && out.size() < max) {
            Map.Entry<String, ArrayDeque<Msg>> entry = it.next();
            ArrayDeque<Msg> group = entry.getValue();
            Msg head;
            // drop expired or dead-lettered heads, then deliver the head only if it is visible:
            // a head that is in flight blocks its whole group, which is what keeps processing in order
            while ((head = group.peekFirst()) != null && head.visibleAt <= now && (expired(head, now) || deadLettered(head, toDeadLetter))) {
                // removed inside expired()/deadLettered(); keep looking at the new head
            }
            if (head != null && head.visibleAt <= now) {
                out.add(deliver(head, now));
                served.add(entry.getKey());
            }
            if (group.isEmpty()) it.remove();
        }
        // Round-robin: groups just served go to the back, so a few busy groups cannot starve the others.
        for (String g : served) {
            ArrayDeque<Msg> d = groups.remove(g);
            if (d != null) groups.put(g, d);
        }
        return out;
    }

    private ReceivedMessage deliver(Msg m, long now) {
        if (m.receipt != null) byReceipt.remove(m.receipt);               // older handles stop working
        m.receiveCount++;
        m.receipt = UUID.randomUUID().toString();
        m.visibleAt = now + config.visibilityTimeoutMs();
        byReceipt.put(m.receipt, m);
        return new ReceivedMessage(m.id, m.body, m.receipt, m.receiveCount, m.groupId);
    }

    /** True (and the message removed here) if it has used up its deliveries and must go to the DLQ. */
    private boolean deadLettered(Msg m, List<Msg> toDeadLetter) {
        if (config.maxReceiveCount() == 0 || m.receiveCount < config.maxReceiveCount()) return false;
        removeEverywhere(m);
        toDeadLetter.add(m);
        return true;
    }

    /** True (and the message removed) if it outlived the retention period. Measured from the FIRST send, as SQS does. */
    private boolean expired(Msg m, long now) {
        if (now - m.firstSentAt < config.retentionMs()) return false;
        removeEverywhere(m);
        return true;
    }

    private void removeEverywhere(Msg m) {
        live.remove(m.id);
        if (m.receipt != null) byReceipt.remove(m.receipt);
        if (config.fifo()) {
            ArrayDeque<Msg> g = groups.get(m.groupId);
            if (g != null) g.remove(m);                                   // empty groups are dropped by the caller:
                                                                           // receiveFifo may be iterating over groups
        } else {
            byVisibleAt.remove(m);
        }
    }

    private long nextVisibleAt() {
        long next = Long.MAX_VALUE;
        if (config.fifo()) {
            for (ArrayDeque<Msg> g : groups.values()) if (!g.isEmpty()) next = Math.min(next, g.peekFirst().visibleAt);
        } else if (!byVisibleAt.isEmpty()) {
            next = byVisibleAt.first().visibleAt;
        }
        return next;
    }

    // ---------------------------------------------------------------------------- acknowledging, visibility

    /** Deletes the message received with this handle. False if the handle is unknown or stale (re-delivered since). */
    public boolean delete(String receiptHandle) {
        lock.lock();
        try {
            Msg m = byReceipt.get(receiptHandle);
            if (m == null) return false;
            removeEverywhere(m);
            if (config.fifo() && groups.containsKey(m.groupId) && groups.get(m.groupId).isEmpty()) groups.remove(m.groupId);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Hides the message for {@code timeoutMs} from now: extend it while a long job is still running, or pass 0 to
     * make it visible immediately (give up and let another consumer try). False if the handle is unknown or stale.
     */
    public boolean changeVisibility(String receiptHandle, long timeoutMs) {
        if (timeoutMs < 0 || timeoutMs > 12 * 3_600_000L) throw new IllegalArgumentException("timeout 0..12h");
        lock.lock();
        try {
            Msg m = byReceipt.get(receiptHandle);
            if (m == null) return false;
            if (!config.fifo()) byVisibleAt.remove(m);                    // must leave the sorted set before its key changes
            m.visibleAt = clock.nowMillis() + timeoutMs;
            if (!config.fifo()) byVisibleAt.add(m);
            changed.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    // ----------------------------------------------------------------------------------------- operations

    /** Moves every message from this queue (normally a DLQ) back to {@code target}. Returns how many moved. */
    public int redriveTo(MessageQueue target) {
        if (target == this) throw new IllegalArgumentException("cannot redrive a queue into itself");
        if (target.config.fifo() != config.fifo()) throw new IllegalArgumentException("queue types differ");
        List<Msg> moving;
        lock.lock();
        try {
            moving = new ArrayList<>(live.values());
            for (Msg m : moving) removeEverywhere(m);
            groups.clear();
        } finally {
            lock.unlock();
        }
        for (Msg m : moving) target.adopt(m.id, m.body, m.groupId, m.firstSentAt);
        return moving.size();
    }

    public QueueStats stats() {
        lock.lock();
        try {
            long now = clock.nowMillis();
            int visible = 0, inFlight = 0, delayed = 0;
            for (Msg m : live.values()) {
                if (now - m.firstSentAt >= config.retentionMs()) continue;
                if (m.visibleAt <= now) visible++;
                else if (m.receipt != null) inFlight++;
                else delayed++;
            }
            if (config.fifo()) {                                           // a message behind an in-flight head is not receivable
                int blocked = 0;
                for (ArrayDeque<Msg> g : groups.values()) {
                    Msg head = g.peekFirst();
                    if (head != null && head.visibleAt > now) {
                        for (Msg m : g) if (m != head && m.visibleAt <= now) blocked++;
                    }
                }
                visible -= blocked;
                delayed += blocked;
            }
            return new QueueStats(visible, inFlight, delayed);
        } finally {
            lock.unlock();
        }
    }

    /** Ids of all messages currently held (for tests and debugging). */
    public Set<String> messageIds() {
        lock.lock();
        try {
            return new HashSet<>(live.keySet());
        } finally {
            lock.unlock();
        }
    }

    private static String sha256(String body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
