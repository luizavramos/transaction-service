package br.com.motorfraude.transaction.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public record HistoryWindow(Instant cutoverAt) {

    public static final Duration LENGTH = Duration.ofDays(90);

    public HistoryWindow {
        Objects.requireNonNull(cutoverAt, "cutoverAt");
    }

    public Instant start() {
        return cutoverAt.minus(LENGTH);
    }

    public Instant end() {
        return cutoverAt;
    }

    public boolean contains(Instant occurredAt) {
        Objects.requireNonNull(occurredAt, "occurredAt");
        return !occurredAt.isBefore(start()) && occurredAt.isBefore(end());
    }
}
