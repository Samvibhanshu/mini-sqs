package dev.samridhi.sqs.server;

import dev.samridhi.sqs.Clock;

/**
 * Starts mini-sqs as a local SQS endpoint.
 * <pre>
 *   java -cp out/main dev.samridhi.sqs.server.Main [port] [--host 0.0.0.0]      defaults: 9324, 127.0.0.1
 * </pre>
 * Then point any AWS SDK at it, for example {@code boto3.client("sqs", endpoint_url="http://127.0.0.1:9324", ...)}.
 */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        int port = 9324;
        String host = "127.0.0.1";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--host") && i + 1 < args.length) host = args[++i];
            else port = Integer.parseInt(args[i]);
        }
        SqsServer server = new SqsServer(new QueueRegistry(Clock.SYSTEM));
        int bound = server.start(host, port);
        System.out.println("mini-sqs listening on http://" + host + ":" + bound + "  (no authentication: keep it on localhost)");
    }
}
