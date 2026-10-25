package dev.samridhi.sqs;

/**
 * A delivery of a message. The {@code receiptHandle} identifies this delivery, not the message: each new delivery
 * gets a new handle, and only the latest one can delete the message or change its visibility.
 */
public record ReceivedMessage(String messageId, String body, String receiptHandle, int receiveCount, String groupId) {}
