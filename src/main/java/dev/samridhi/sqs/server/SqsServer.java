package dev.samridhi.sqs.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.samridhi.sqs.MessageQueue;
import dev.samridhi.sqs.ReceivedMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

/**
 * Mini-SQS over HTTP, speaking the SQS JSON protocol: {@code POST /} with an {@code X-Amz-Target: AmazonSQS.<Action>}
 * header and a JSON body, the way the AWS SDKs talk to the real service. So boto3 (and other SDKs) can use it as a
 * local SQS by pointing their endpoint at it.
 *
 * <p>Requests are not authenticated (signatures are ignored), so the server binds to 127.0.0.1 unless told otherwise.
 *
 * <p><b>Long polling holds a thread.</b> A receive with {@code WaitTimeSeconds} parks its request thread until a message
 * arrives or the wait ends. At most {@value #MAX_LONG_POLLS} receives (by default) wait at once; beyond that a receive returns
 * immediately (an empty answer is always allowed in SQS), so waiting consumers can never use up the server.
 */
public final class SqsServer {
    public static final int MAX_BODY_BYTES = 1 << 20;
    public static final int MAX_LONG_POLLS = 256;
    private static final Pattern BATCH_ID = Pattern.compile("[A-Za-z0-9_-]{1,80}");

    static {
        // Without TCP_NODELAY the JDK server's responses wait for the client's delayed ACK: about 40 ms each.
        if (System.getProperty("sun.net.httpserver.nodelay") == null) System.setProperty("sun.net.httpserver.nodelay", "true");
    }

    private final QueueRegistry registry;
    private final Semaphore longPolls;
    private HttpServer server;
    private ExecutorService pool;

    public SqsServer(QueueRegistry registry) {
        this(registry, MAX_LONG_POLLS);
    }

    public SqsServer(QueueRegistry registry, int maxLongPolls) {
        this.registry = registry;
        this.longPolls = new Semaphore(maxLongPolls);
    }

    public int start(String host, int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(host, port), 256);
        pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "sqs-http");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(pool);
        server.createContext("/", this::handle);
        server.start();
        int bound = server.getAddress().getPort();
        registry.baseUrl("http://" + (host.equals("0.0.0.0") ? "localhost" : host) + ":" + bound);
        return bound;
    }

    public void stop() {
        if (server != null) server.stop(0);
        if (pool != null) pool.shutdownNow();
    }

    /** Receives currently allowed to long-poll (for tests). */
    int longPollPermits() {
        return longPolls.availablePermits();
    }

    // ------------------------------------------------------------------------------------------------ HTTP

    private void handle(HttpExchange ex) throws IOException {
        int status = 200;
        Object out;
        String queryError = null;
        try {
            if (!ex.getRequestMethod().equals("POST")) throw SqsError.unknownOperation(ex.getRequestMethod() + " " + ex.getRequestURI().getPath());
            String target = ex.getRequestHeaders().getFirst("X-Amz-Target");
            if (target == null || !target.startsWith("AmazonSQS.")) throw SqsError.unknownOperation(String.valueOf(target));
            Map<String, Object> in = body(ex);
            out = dispatch(target.substring("AmazonSQS.".length()), in, ex.getRequestHeaders().getFirst("Host"));
        } catch (SqsError e) {
            status = e.status;
            out = error(e.type, e.getMessage());
            queryError = e.queryCode + ";" + (e.status >= 500 ? "Receiver" : "Sender");
        } catch (IllegalArgumentException e) {                  // validation in MessageQueue, or malformed JSON
            status = 400;
            out = error("InvalidParameterValue", e.getMessage());
            queryError = "InvalidParameterValue;Sender";
        } catch (RuntimeException e) {
            status = 500;
            out = error("InternalError", "Internal error.");      // never leak stack traces
            queryError = "InternalError;Receiver";
        }
        byte[] bytes = Json.write(out).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
        ex.getResponseHeaders().set("x-amzn-RequestId", UUID.randomUUID().toString());
        if (queryError != null) ex.getResponseHeaders().set("x-amzn-query-error", queryError);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static Map<String, Object> error(String type, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("__type", "com.amazonaws.sqs#" + type);
        m.put("message", message);
        return m;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(HttpExchange ex) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (InputStream in = ex.getRequestBody()) {
            byte[] chunk = new byte[8192];
            int n;
            while ((n = in.read(chunk)) > 0) {
                buf.write(chunk, 0, n);
                if (buf.size() > MAX_BODY_BYTES) throw SqsError.tooLarge();
            }
        }
        if (buf.size() == 0) return new LinkedHashMap<>();
        Object v = Json.parse(buf.toString(StandardCharsets.UTF_8));
        if (!(v instanceof Map)) throw SqsError.invalidParameter("The request body must be a JSON object.");
        return (Map<String, Object>) v;
    }

    // ------------------------------------------------------------------------------------------------ actions

    private Object dispatch(String action, Map<String, Object> in, String host) {
        return switch (action) {
            case "CreateQueue" -> {
                if (in.get("tags") != null) throw SqsError.unsupported("Queue tags");
                QueueRegistry.Queue q = registry.create(str(in, "QueueName", true), stringMap(in.get("Attributes")));
                yield Map.of("QueueUrl", url(host, q.name()));
            }
            case "GetQueueUrl" -> Map.of("QueueUrl", url(host, registry.byName(str(in, "QueueName", true)).name()));
            case "ListQueues" -> {
                List<String> urls = new ArrayList<>();
                for (String u : registry.list(str(in, "QueueNamePrefix", false))) urls.add(url(host, u.substring(u.lastIndexOf('/') + 1)));
                yield urls.isEmpty() ? Map.of() : Map.of("QueueUrls", urls);
            }
            case "DeleteQueue" -> {
                registry.delete(registry.byUrl(str(in, "QueueUrl", true)).name());
                yield Map.of();
            }
            case "GetQueueAttributes" -> getAttributes(in);
            case "SendMessage" -> {
                QueueRegistry.Queue q = queue(in);
                yield sent(q, send(q, in));
            }
            case "SendMessageBatch" -> sendBatch(in);
            case "ReceiveMessage" -> receive(in);
            case "DeleteMessage" -> {
                if (!queue(in).queue().delete(str(in, "ReceiptHandle", true))) throw SqsError.receiptHandleIsInvalid();
                yield Map.of();
            }
            case "DeleteMessageBatch" -> handleBatch(in, (q, e) -> q.queue().delete(str(e, "ReceiptHandle", true)));
            case "ChangeMessageVisibility" -> {
                QueueRegistry.Queue q = queue(in);
                int seconds = intParam(in, "VisibilityTimeout", 0, 43_200, -1);
                if (seconds < 0) throw SqsError.missingParameter("VisibilityTimeout");
                if (!q.queue().changeVisibility(str(in, "ReceiptHandle", true), seconds * 1000L)) throw SqsError.receiptHandleIsInvalid();
                yield Map.of();
            }
            case "ChangeMessageVisibilityBatch" -> handleBatch(in, (q, e) -> {
                int seconds = intParam(e, "VisibilityTimeout", 0, 43_200, -1);
                if (seconds < 0) throw SqsError.missingParameter("VisibilityTimeout");
                return q.queue().changeVisibility(str(e, "ReceiptHandle", true), seconds * 1000L);
            });
            case "ListDeadLetterSourceQueues" -> {
                List<String> urls = new ArrayList<>();
                for (QueueRegistry.Queue s : registry.sourcesOf(queue(in))) urls.add(url(host, s.name()));
                yield Map.of("queueUrls", urls);
            }
            case "StartMessageMoveTask" -> moveMessages(in);
            case "AddPermission", "RemovePermission", "TagQueue", "UntagQueue", "ListQueueTags", "SetQueueAttributes",
                 "PurgeQueue", "CancelMessageMoveTask", "ListMessageMoveTasks" -> throw SqsError.unsupported(action);
            default -> throw SqsError.unknownOperation(action);
        };
    }

    private static String url(String host, String name) {
        return "http://" + (host == null ? "localhost" : host) + "/" + QueueRegistry.ACCOUNT + "/" + name;
    }

    private QueueRegistry.Queue queue(Map<String, Object> in) {
        return registry.byUrl(str(in, "QueueUrl", true));
    }

    private Map<String, Object> getAttributes(Map<String, Object> in) {
        QueueRegistry.Queue q = queue(in);
        Map<String, String> all = registry.describe(q);
        Set<String> wanted = new HashSet<>(stringList(in.get("AttributeNames")));
        Map<String, String> out = new LinkedHashMap<>();
        for (String name : wanted) {
            if (!name.equals("All") && !all.containsKey(name) && !KNOWN_ATTRIBUTES.contains(name)) throw SqsError.invalidAttributeName(name);
        }
        for (Map.Entry<String, String> e : all.entrySet()) {
            if (wanted.contains("All") || wanted.contains(e.getKey())) out.put(e.getKey(), e.getValue());
        }
        return out.isEmpty() ? Map.of() : Map.of("Attributes", out);
    }

    /** Real SQS attributes this server does not have (it answers without them rather than failing). */
    private static final Set<String> KNOWN_ATTRIBUTES = Set.of("Policy", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds",
            "SqsManagedSseEnabled", "DeduplicationScope", "FifoThroughputLimit", "RedriveAllowPolicy");

    private static Map<String, Object> sent(QueueRegistry.Queue q, String[] idAndBody) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("MessageId", idAndBody[0]);
        m.put("MD5OfMessageBody", md5(idAndBody[1]));
        return m;
    }

    /** Validates one message the way SQS does and sends it. Returns {message id, body}. */
    private static String[] send(QueueRegistry.Queue q, Map<String, Object> in) {
        String body = str(in, "MessageBody", true);
        if (in.get("MessageAttributes") instanceof Map<?, ?> attrs && !attrs.isEmpty()) throw SqsError.unsupported("MessageAttributes");
        if (in.get("MessageSystemAttributes") instanceof Map<?, ?> attrs && !attrs.isEmpty()) throw SqsError.unsupported("MessageSystemAttributes");
        checkCharacters(body);
        String group = str(in, "MessageGroupId", false);
        String dedup = str(in, "MessageDeduplicationId", false);
        long delayMs;
        if (q.fifo()) {
            if (group == null) throw SqsError.missingParameter("MessageGroupId");
            if (dedup == null && !q.contentBasedDedup()) {
                throw SqsError.invalidParameter("The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly.");
            }
            if (in.get("DelaySeconds") != null) throw SqsError.invalidParameter("FIFO queues do not support DelaySeconds per message; set it on the queue.");
            delayMs = q.delayMs();
        } else {
            if (group != null || dedup != null) throw SqsError.invalidParameter("MessageGroupId and MessageDeduplicationId are only used with FIFO queues.");
            int d = intParam(in, "DelaySeconds", 0, 900, -1);
            delayMs = d < 0 ? q.delayMs() : d * 1000L;
        }
        String id = q.queue().send(body, delayMs, group, dedup);
        return new String[]{id, body};
    }

    /** SQS accepts only these Unicode characters in a body: #x9 #xA #xD #x20-#xD7FF #xE000-#xFFFD #x10000-#x10FFFF. */
    private static void checkCharacters(String body) {
        for (int i = 0; i < body.length(); ) {
            int c = body.codePointAt(i);
            boolean ok = c == 0x9 || c == 0xA || c == 0xD || (c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) || c >= 0x10000;
            if (!ok) throw SqsError.invalidParameter("The message contains characters outside the allowed set (code point " + c + ").");
            i += Character.charCount(c);
        }
    }

    private Map<String, Object> sendBatch(Map<String, Object> in) {
        QueueRegistry.Queue q = queue(in);
        List<Map<String, Object>> entries = entries(in);
        long total = 0;
        for (Map<String, Object> e : entries) total += String.valueOf(e.get("MessageBody")).getBytes(StandardCharsets.UTF_8).length;
        if (total > 262_144) throw SqsError.invalidParameter("The batch's messages add up to more than 262,144 bytes.");
        List<Object> ok = new ArrayList<>(), failed = new ArrayList<>();
        for (Map<String, Object> e : entries) {
            try {
                Map<String, Object> m = new LinkedHashMap<>(sent(q, send(q, e)));
                m.put("Id", e.get("Id"));
                ok.add(m);
            } catch (SqsError err) {
                failed.add(failure(e, err.type, err.getMessage()));
            } catch (IllegalArgumentException err) {
                failed.add(failure(e, "InvalidParameterValue", err.getMessage()));
            }
        }
        return batchResult(ok, failed);
    }

    private interface EntryAction {
        boolean apply(QueueRegistry.Queue q, Map<String, Object> entry);
    }

    /** Delete and change-visibility batches: each entry succeeds or fails with ReceiptHandleIsInvalid on its own. */
    private Map<String, Object> handleBatch(Map<String, Object> in, EntryAction action) {
        QueueRegistry.Queue q = queue(in);
        List<Object> ok = new ArrayList<>(), failed = new ArrayList<>();
        for (Map<String, Object> e : entries(in)) {
            try {
                if (action.apply(q, e)) ok.add(Map.of("Id", e.get("Id")));
                else failed.add(failure(e, "ReceiptHandleIsInvalid", "The receipt handle is not valid."));
            } catch (SqsError err) {
                failed.add(failure(e, err.type, err.getMessage()));
            }
        }
        return batchResult(ok, failed);
    }

    private static List<Map<String, Object>> entries(Map<String, Object> in) {
        List<Map<String, Object>> entries = new ArrayList<>();
        if (in.get("Entries") instanceof List<?> l) {
            for (Object o : l) {
                if (!(o instanceof Map<?, ?> m)) throw SqsError.invalidParameter("Each batch entry must be an object.");
                @SuppressWarnings("unchecked")
                Map<String, Object> e = (Map<String, Object>) m;
                entries.add(e);
            }
        }
        if (entries.isEmpty()) throw SqsError.emptyBatch();
        if (entries.size() > 10) throw SqsError.tooManyEntries();
        Set<String> ids = new HashSet<>();
        for (Map<String, Object> e : entries) {
            if (!(e.get("Id") instanceof String id) || !BATCH_ID.matcher(id).matches()) throw SqsError.invalidBatchEntryId();
            if (!ids.add(id)) throw SqsError.batchIdsNotDistinct();
        }
        return entries;
    }

    private static Map<String, Object> failure(Map<String, Object> entry, String code, String message) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("Id", entry.get("Id"));
        f.put("SenderFault", true);
        f.put("Code", code);
        f.put("Message", message);
        return f;
    }

    private static Map<String, Object> batchResult(List<Object> ok, List<Object> failed) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("Successful", ok);
        m.put("Failed", failed);
        return m;
    }

    private Map<String, Object> receive(Map<String, Object> in) {
        QueueRegistry.Queue q = queue(in);
        int max = intParam(in, "MaxNumberOfMessages", 1, 10, 1);
        int wait = intParam(in, "WaitTimeSeconds", 0, 20, q.waitSeconds());
        int visibility = intParam(in, "VisibilityTimeout", 0, 43_200, -1);
        Set<String> wanted = new HashSet<>(stringList(in.get("AttributeNames")));
        wanted.addAll(stringList(in.get("MessageSystemAttributeNames")));
        List<ReceivedMessage> got;
        if (wait > 0 && longPolls.tryAcquire()) {
            try {
                got = q.queue().receive(max, wait * 1000L);
            } finally {
                longPolls.release();
            }
        } else {
            got = q.queue().receive(max, 0);                           // too many waiting already: answer now
        }
        List<Object> messages = new ArrayList<>();
        for (ReceivedMessage m : got) {
            if (visibility >= 0) q.queue().changeVisibility(m.receiptHandle(), visibility * 1000L);
            Map<String, Object> j = new LinkedHashMap<>();
            j.put("MessageId", m.messageId());
            j.put("ReceiptHandle", m.receiptHandle());
            j.put("MD5OfBody", md5(m.body()));
            j.put("Body", m.body());
            Map<String, Object> attrs = new LinkedHashMap<>();
            boolean all = wanted.contains("All");
            if (all || wanted.contains("ApproximateReceiveCount")) attrs.put("ApproximateReceiveCount", Integer.toString(m.receiveCount()));
            if (m.groupId() != null && (all || wanted.contains("MessageGroupId"))) attrs.put("MessageGroupId", m.groupId());
            if (!attrs.isEmpty()) j.put("Attributes", attrs);
            messages.add(j);
        }
        return messages.isEmpty() ? Map.of() : Map.of("Messages", messages);
    }

    /** Redrive: moves every message from a dead-letter queue back to a queue that uses it (or a named destination). */
    private Map<String, Object> moveMessages(Map<String, Object> in) {
        QueueRegistry.Queue source = registry.byArn(str(in, "SourceArn", true));
        if (source == null) throw SqsError.invalidParameter("SourceArn is not a queue on this server.");
        List<QueueRegistry.Queue> sources = registry.sourcesOf(source);
        if (sources.isEmpty()) throw SqsError.invalidParameter("The source queue is not a dead-letter queue of any queue.");
        QueueRegistry.Queue dest;
        String destArn = str(in, "DestinationArn", false);
        if (destArn != null) {
            dest = registry.byArn(destArn);
            if (dest == null) throw SqsError.invalidParameter("DestinationArn is not a queue on this server.");
        } else if (sources.size() == 1) {
            dest = sources.get(0);
        } else {
            throw SqsError.invalidParameter("This dead-letter queue serves several queues; give a DestinationArn.");
        }
        MessageQueue from = source.queue();
        int moved = from.redriveTo(dest.queue());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("TaskHandle", UUID.randomUUID().toString());
        m.put("ApproximateNumberOfMessagesMoved", moved);              // not in SQS's response; harmless to SDKs
        return m;
    }

    // ------------------------------------------------------------------------------------------------ params

    private static String str(Map<String, Object> in, String name, boolean required) {
        Object v = in.get(name);
        if (v == null) {
            if (required) throw SqsError.missingParameter(name);
            return null;
        }
        if (!(v instanceof String s)) throw SqsError.invalidParameter(name + " must be a string.");
        return s;
    }

    private static int intParam(Map<String, Object> in, String name, int lo, int hi, int dflt) {
        Object v = in.get(name);
        if (v == null) return dflt;
        if (v instanceof Number n && n.doubleValue() == n.longValue() && n.longValue() >= lo && n.longValue() <= hi) return (int) n.longValue();
        throw SqsError.invalidParameter(name + " must be an integer from " + lo + " to " + hi + ".");
    }

    private static Map<String, String> stringMap(Object v) {
        Map<String, String> out = new LinkedHashMap<>();
        if (v == null) return out;
        if (!(v instanceof Map<?, ?> m)) throw SqsError.invalidParameter("Attributes must be an object.");
        for (Map.Entry<?, ?> e : m.entrySet()) out.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        return out;
    }

    private static List<String> stringList(Object v) {
        List<String> out = new ArrayList<>();
        if (v instanceof List<?> l) for (Object o : l) out.add(String.valueOf(o));
        return out;
    }

    static String md5(String body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
