package dev.samridhi.sqs;

import java.util.concurrent.atomic.AtomicLong;

/** Time source. Tests use {@link Manual} so that "wait 30 seconds" takes no real time and is fully repeatable. */
public interface Clock {
    long nowMillis();

    Clock SYSTEM = System::currentTimeMillis;

    final class Manual implements Clock {
        private final AtomicLong now;

        public Manual(long startMillis) {
            now = new AtomicLong(startMillis);
        }

        @Override
        public long nowMillis() {
            return now.get();
        }

        public void advance(long millis) {
            if (millis < 0) throw new IllegalArgumentException("time only moves forward");
            now.addAndGet(millis);
        }
    }
}
