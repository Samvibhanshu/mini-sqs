package dev.samridhi.sqs.server;

/**
 * An error in SQS's own vocabulary, so SDK clients can catch it by name. Sent as HTTP 400 (or 500) with
 * {@code {"__type": "com.amazonaws.sqs#<type>", "message": ...}} and the legacy code in the {@code x-amzn-query-error}
 * header, which is how SQS reports errors on its JSON protocol.
 */
public final class SqsError extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public final String type;          // the modelled error name, e.g. QueueDoesNotExist
    public final String queryCode;     // the older code SDKs also understand, e.g. AWS.SimpleQueueService.NonExistentQueue
    public final int status;

    private SqsError(String type, String queryCode, int status, String message) {
        super(message);
        this.type = type;
        this.queryCode = queryCode;
        this.status = status;
    }

    public static SqsError queueDoesNotExist() {
        return new SqsError("QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue", 400, "The specified queue does not exist.");
    }

    public static SqsError queueNameExists(String name) {
        return new SqsError("QueueNameExists", "QueueAlreadyExists", 400, "A queue named " + name + " already exists with different attributes.");
    }

    public static SqsError receiptHandleIsInvalid() {
        return new SqsError("ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid", 400,
                "The receipt handle is not valid: it was never issued, the message was deleted, or it was received again since.");
    }

    public static SqsError invalidParameter(String message) {
        return new SqsError("InvalidParameterValue", "InvalidParameterValue", 400, message);
    }

    public static SqsError missingParameter(String name) {
        return new SqsError("MissingParameter", "MissingParameter", 400, "The request must contain the parameter " + name + ".");
    }

    public static SqsError invalidAttributeName(String name) {
        return new SqsError("InvalidAttributeName", "InvalidAttributeName", 400, "Unknown attribute " + name + ".");
    }

    public static SqsError unsupported(String what) {
        return new SqsError("UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation", 400, what + " is not supported by mini-sqs.");
    }

    public static SqsError unknownOperation(String target) {
        return new SqsError("UnknownOperationException", "UnknownOperationException", 400, "Unknown operation " + target + ".");
    }

    public static SqsError tooManyEntries() {
        return new SqsError("TooManyEntriesInBatchRequest", "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", 400, "A batch can hold at most 10 entries.");
    }

    public static SqsError emptyBatch() {
        return new SqsError("EmptyBatchRequest", "AWS.SimpleQueueService.EmptyBatchRequest", 400, "The batch request contains no entries.");
    }

    public static SqsError batchIdsNotDistinct() {
        return new SqsError("BatchEntryIdsNotDistinct", "AWS.SimpleQueueService.BatchEntryIdsNotDistinct", 400, "Two or more batch entries have the same Id.");
    }

    public static SqsError invalidBatchEntryId() {
        return new SqsError("InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId", 400, "A batch entry Id must be 1-80 letters, digits, hyphens or underscores.");
    }

    public static SqsError tooLarge() {
        return new SqsError("InvalidParameterValue", "InvalidParameterValue", 413, "The request body is too large.");
    }

    public static SqsError internal() {
        return new SqsError("InternalError", "InternalError", 500, "Internal error.");
    }
}
