package dev.samridhi.sqs.client;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import dev.samridhi.sqs.server.Json;

/**
 * A small Java client for mini-sqs (or real SQS's JSON protocol, minus request signing). One method per action.
 * Errors come back as {@link Error} with the SQS error name, e.g. {@code QueueDoesNotExist}.
 */
public final class SqsClient {
    public record Message(String messageId, String body, String receiptHandle, int receiveCount, String groupId) {}

    public static final class Error extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public final String code;
        public final int status;

        Error(String code, int status, String message) {
            super(code + ": " + message);
            this.code = code;
            this.status = status;
        }
    }

    private final URI endpoint;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public SqsClient(String endpoint) {
        this.endpoint = URI.create(endpoint);
    }

    public String createQueue(String name, Map<String, String> attributes) {
        return string(call("CreateQueue", map("QueueName", name, "Attributes", attributes)), "QueueUrl");
    }

    public String queueUrl(String name) {
        return string(call("GetQueueUrl", map("QueueName", name)), "QueueUrl");
    }

    public List<String> listQueues(String prefix) {
        Object urls = call("ListQueues", prefix == null ? Map.of() : map("QueueNamePrefix", prefix)).get("QueueUrls");
        List<String> out = new ArrayList<>();
        if (urls instanceof List<?> l) for (Object o : l) out.add((String) o);
        return out;
    }

    public void deleteQueue(String queueUrl) {
        call("DeleteQueue", map("QueueUrl", queueUrl));
    }

    public String send(String queueUrl, String body) {
        return string(call("SendMessage", map("QueueUrl", queueUrl, "MessageBody", body)), "MessageId");
    }

    public String sendFifo(String queueUrl, String body, String groupId, String dedupId) {
        Map<String, Object> in = map("QueueUrl", queueUrl, "MessageBody", body, "MessageGroupId", groupId);
        if (dedupId != null) in.put("MessageDeduplicationId", dedupId);
        return string(call("SendMessage", in), "MessageId");
    }

    public String sendDelayed(String queueUrl, String body, int delaySeconds) {
        return string(call("SendMessage", map("QueueUrl", queueUrl, "MessageBody", body, "DelaySeconds", delaySeconds)), "MessageId");
    }

    /** Up to {@code max} messages, waiting up to {@code waitSeconds} for the first one (long polling). */
    public List<Message> receive(String queueUrl, int max, int waitSeconds) {
        Map<String, Object> out = call("ReceiveMessage", map("QueueUrl", queueUrl, "MaxNumberOfMessages", max,
                "WaitTimeSeconds", waitSeconds, "MessageSystemAttributeNames", List.of("All")));
        List<Message> msgs = new ArrayList<>();
        if (out.get("Messages") instanceof List<?> l) {
            for (Object o : l) {
                Map<?, ?> m = (Map<?, ?>) o;
                Map<?, ?> a = m.get("Attributes") instanceof Map<?, ?> x ? x : Map.of();
                msgs.add(new Message((String) m.get("MessageId"), (String) m.get("Body"), (String) m.get("ReceiptHandle"),
                        Integer.parseInt(String.valueOf(a.get("ApproximateReceiveCount") == null ? 0 : a.get("ApproximateReceiveCount"))),
                        (String) a.get("MessageGroupId")));
            }
        }
        return msgs;
    }

    public void delete(String queueUrl, String receiptHandle) {
        call("DeleteMessage", map("QueueUrl", queueUrl, "ReceiptHandle", receiptHandle));
    }

    public void changeVisibility(String queueUrl, String receiptHandle, int seconds) {
        call("ChangeMessageVisibility", map("QueueUrl", queueUrl, "ReceiptHandle", receiptHandle, "VisibilityTimeout", seconds));
    }

    public Map<String, String> attributes(String queueUrl) {
        Map<String, String> out = new LinkedHashMap<>();
        if (call("GetQueueAttributes", map("QueueUrl", queueUrl, "AttributeNames", List.of("All"))).get("Attributes") instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) out.put((String) e.getKey(), (String) e.getValue());
        }
        return out;
    }

    /** Moves every message in a dead-letter queue back to its source queue. */
    public void redrive(String dlqArn) {
        call("StartMessageMoveTask", map("SourceArn", dlqArn));
    }

    /** Sends one action. Public so tests can send anything, including malformed requests. */
    public Map<String, Object> call(String action, Map<String, ?> input) {
        String json = Json.write(input);
        HttpRequest req = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/x-amz-json-1.0")
                .header("X-Amz-Target", "AmazonSQS." + action)
                .POST(HttpRequest.BodyPublishers.ofString(json)).build();
        HttpResponse<String> res;
        try {
            res = http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) Json.parse(res.body());
        if (res.statusCode() != 200) {
            String type = String.valueOf(body.get("__type"));
            throw new Error(type.substring(type.indexOf('#') + 1), res.statusCode(), String.valueOf(body.get("message")));
        }
        return body;
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) if (kv[i + 1] != null) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private static String string(Map<String, Object> m, String key) {
        return (String) m.get(key);
    }
}
