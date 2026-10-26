package dev.samridhi.sqs.server;

import dev.samridhi.sqs.Clock;
import dev.samridhi.sqs.MessageQueue;
import dev.samridhi.sqs.QueueConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * The server's queues by name, with their SQS-style attributes. Turns SQS attributes (seconds, strings, a JSON redrive
 * policy) into a {@link QueueConfig} and checks them the way SQS does.
 */
public final class QueueRegistry {
    public static final String ACCOUNT = "000000000000";
    public static final String REGION = "local";
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_-]{1,80}");

    /** A queue plus what SQS remembers about it. {@code attributes} holds every settable attribute, defaults filled in. */
    public record Queue(String name, MessageQueue queue, Map<String, String> attributes, long createdAtSeconds) {
        public long delayMs() {
            return Long.parseLong(attributes.get("DelaySeconds")) * 1000;
        }

        public int waitSeconds() {
            return Integer.parseInt(attributes.get("ReceiveMessageWaitTimeSeconds"));
        }

        public boolean fifo() {
            return queue.config().fifo();
        }

        public boolean contentBasedDedup() {
            return "true".equals(attributes.get("ContentBasedDeduplication"));
        }
    }

    private final ConcurrentHashMap<String, Queue> byName = new ConcurrentHashMap<>();
    private final Clock clock;
    private volatile String baseUrl = "http://127.0.0.1";

    public QueueRegistry(Clock clock) {
        this.clock = clock;
    }

    /** Set by the server once it knows its port; queue URLs are built from it. */
    public void baseUrl(String url) {
        this.baseUrl = url;
    }

    public String url(String name) {
        return baseUrl + "/" + ACCOUNT + "/" + name;
    }

    public static String arn(String name) {
        return "arn:aws:sqs:" + REGION + ":" + ACCOUNT + ":" + name;
    }

    /** Creates the queue, or returns the existing one if its attributes are the same (CreateQueue is idempotent). */
    public synchronized Queue create(String name, Map<String, String> requested) {
        if (name == null) throw SqsError.missingParameter("QueueName");
        boolean fifoName = name.endsWith(".fifo");
        String base = fifoName ? name.substring(0, name.length() - 5) : name;
        if (!NAME.matcher(base).matches() || name.length() > 80) {
            throw SqsError.invalidParameter("Queue names are 1-80 letters, digits, hyphens and underscores (FIFO names end in .fifo).");
        }
        Map<String, String> attrs = normalise(requested, fifoName);
        Queue existing = byName.get(name);
        if (existing != null) {
            if (!existing.attributes().equals(attrs)) throw SqsError.queueNameExists(name);
            return existing;
        }
        MessageQueue dlq = null;
        int maxReceives = 0;
        if (attrs.containsKey("RedrivePolicy")) {
            Map<?, ?> policy;
            try {
                policy = (Map<?, ?>) Json.parse(attrs.get("RedrivePolicy"));
            } catch (RuntimeException e) {
                throw SqsError.invalidParameter("RedrivePolicy must be a JSON object.");
            }
            Queue target = byArn(String.valueOf(policy.get("deadLetterTargetArn")));
            if (target == null) throw SqsError.invalidParameter("The dead-letter queue in RedrivePolicy does not exist.");
            if (target.fifo() != fifoName) throw SqsError.invalidParameter("A FIFO queue needs a FIFO dead-letter queue, and a standard one a standard one.");
            maxReceives = intIn(String.valueOf(policy.get("maxReceiveCount")), 1, 1000, "maxReceiveCount");
            dlq = target.queue();
        }
        long visibilityMs = Long.parseLong(attrs.get("VisibilityTimeout")) * 1000;
        long retentionMs = Long.parseLong(attrs.get("MessageRetentionPeriod")) * 1000;
        QueueConfig config = new QueueConfig(visibilityMs, maxReceives, dlq, retentionMs, fifoName, fifoName ? 5 * 60_000L : 0);
        Queue q = new Queue(name, new MessageQueue(name, config, clock), attrs, clock.nowMillis() / 1000);
        byName.put(name, q);
        return q;
    }

    private static Map<String, String> normalise(Map<String, String> requested, boolean fifoName) {
        Map<String, String> a = new TreeMap<>();
        a.put("VisibilityTimeout", "30");
        a.put("MessageRetentionPeriod", "345600");
        a.put("DelaySeconds", "0");
        a.put("ReceiveMessageWaitTimeSeconds", "0");
        a.put("MaximumMessageSize", "262144");
        if (fifoName) {
            a.put("FifoQueue", "true");
            a.put("ContentBasedDeduplication", "false");
        }
        for (Map.Entry<String, String> e : (requested == null ? Map.<String, String>of() : requested).entrySet()) {
            String k = e.getKey(), v = e.getValue();
            switch (k) {
                case "VisibilityTimeout" -> a.put(k, Integer.toString(intIn(v, 0, 43_200, k)));
                case "MessageRetentionPeriod" -> a.put(k, Integer.toString(intIn(v, 60, 1_209_600, k)));
                case "DelaySeconds" -> a.put(k, Integer.toString(intIn(v, 0, 900, k)));
                case "ReceiveMessageWaitTimeSeconds" -> a.put(k, Integer.toString(intIn(v, 0, 20, k)));
                case "MaximumMessageSize" -> {
                    if (intIn(v, 1024, 262_144, k) != 262_144) throw SqsError.unsupported("A MaximumMessageSize other than 262144");
                }
                case "RedrivePolicy" -> a.put(k, v);
                case "FifoQueue" -> {
                    if (!v.equals(Boolean.toString(fifoName))) throw SqsError.invalidParameter("FifoQueue=true needs a queue name ending in .fifo, and only then.");
                }
                case "ContentBasedDeduplication" -> {
                    if (!fifoName) throw SqsError.invalidAttributeName(k);
                    if (!v.equals("true") && !v.equals("false")) throw SqsError.invalidParameter(k + " must be true or false.");
                    a.put(k, v);
                }
                default -> throw SqsError.invalidAttributeName(k);
            }
        }
        return a;
    }

    static int intIn(String v, int lo, int hi, String name) {
        try {
            int n = Integer.parseInt(v.trim());
            if (n >= lo && n <= hi) return n;
        } catch (RuntimeException ignored) {
            // falls through to the error below
        }
        throw SqsError.invalidParameter(name + " must be an integer from " + lo + " to " + hi + ".");
    }

    public Queue byName(String name) {
        Queue q = name == null ? null : byName.get(name);
        if (q == null) throw SqsError.queueDoesNotExist();
        return q;
    }

    /** Accepts the full queue URL (any host, since clients may reach the server by another name) or a bare name. */
    public Queue byUrl(String url) {
        if (url == null) throw SqsError.missingParameter("QueueUrl");
        return byName(url.substring(url.lastIndexOf('/') + 1));
    }

    public Queue byArn(String arn) {
        String prefix = "arn:aws:sqs:" + REGION + ":" + ACCOUNT + ":";
        if (arn == null || !arn.startsWith(prefix)) return null;
        return byName.get(arn.substring(prefix.length()));
    }

    public List<String> list(String prefix) {
        List<String> out = new ArrayList<>();
        for (String n : new TreeMap<>(byName).keySet()) if (prefix == null || n.startsWith(prefix)) out.add(url(n));
        return out;
    }

    /** Queues whose redrive policy sends to {@code dlq}. */
    public List<Queue> sourcesOf(Queue dlq) {
        List<Queue> out = new ArrayList<>();
        for (Queue q : new TreeMap<>(byName).values()) {
            if (q.queue().config().deadLetterQueue() == dlq.queue()) out.add(q);
        }
        return out;
    }

    public synchronized void delete(String name) {
        if (byName.remove(name) == null) throw SqsError.queueDoesNotExist();
    }

    /** All attributes SQS would report for GetQueueAttributes with "All". */
    public Map<String, String> describe(Queue q) {
        Map<String, String> out = new LinkedHashMap<>(q.attributes());
        dev.samridhi.sqs.QueueStats s = q.queue().stats();
        out.put("ApproximateNumberOfMessages", Integer.toString(s.visible()));
        out.put("ApproximateNumberOfMessagesNotVisible", Integer.toString(s.inFlight()));
        out.put("ApproximateNumberOfMessagesDelayed", Integer.toString(s.delayed()));
        out.put("QueueArn", arn(q.name()));
        out.put("CreatedTimestamp", Long.toString(q.createdAtSeconds()));
        out.put("LastModifiedTimestamp", Long.toString(q.createdAtSeconds()));
        return out;
    }
}
