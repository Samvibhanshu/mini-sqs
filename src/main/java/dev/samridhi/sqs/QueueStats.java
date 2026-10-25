package dev.samridhi.sqs;

/** Approximate counts, as SQS reports them: ready to receive, received but not yet deleted, and delayed. */
public record QueueStats(int visible, int inFlight, int delayed) {
    public int total() {
        return visible + inFlight + delayed;
    }
}
