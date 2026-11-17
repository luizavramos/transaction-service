package br.com.motorfraude.transaction.infrastructure;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.transaction.application.usecase.SkipReason;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackfillStatus {

    private static final Logger LOG = LoggerFactory.getLogger(BackfillStatus.class);

    public record Snapshot(
            String state,
            Instant cutoverAt,
            Instant windowStart,
            long published,
            Map<String, Long> skipped,
            long malformed,
            Instant completedAt) {
    }

    private final Instant cutoverAt;
    private final Clock clock;
    private final Path file;
    private final Duration quiet;

    private long published;
    private long malformed;
    private final Map<SkipReason, Long> skipped = new EnumMap<>(SkipReason.class);
    private boolean lastSeen;
    private Instant lastEventAt;
    private Snapshot restored;
    private boolean persisted;

    public BackfillStatus(Instant cutoverAt, Clock clock, Path file) {
        this(cutoverAt, clock, file, Duration.ZERO);
    }

    public BackfillStatus(Instant cutoverAt, Clock clock, Path file, Duration quiet) {
        this.cutoverAt = cutoverAt;
        this.clock = clock;
        this.file = file;
        this.quiet = quiet;
        for (var reason : SkipReason.values()) {
            skipped.put(reason, 0L);
        }
        load();
    }

    public synchronized void published() {
        published++;
        touch();
    }

    public synchronized void skipped(SkipReason reason) {
        skipped.merge(reason, 1L, Long::sum);
        touch();
    }

    public synchronized void malformed() {
        malformed++;
        touch();
    }

    public synchronized void snapshotCompleted() {
        lastSeen = true;
        lastEventAt = clock.instant();
    }

    public synchronized Snapshot snapshot() {
        if (restored != null) {
            return restored;
        }
        var completed = lastSeen && !clock.instant().isBefore(lastEventAt.plus(quiet));
        var snapshot = build(completed ? "COMPLETED" : "RUNNING", completed ? lastEventAt : null);
        if (completed && !persisted) {
            persist(snapshot);
        }
        return snapshot;
    }

    public String toJson() {
        return JsonCodec.toJson(snapshot());
    }

    private void touch() {
        lastEventAt = clock.instant();
        if (persisted) {
            persisted = false;
            if (file == null) {
                return;
            }
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                LOG.warn("não foi possível apagar {}: {}", file, e.getMessage());
            }
        }
    }

    private Snapshot build(String state, Instant completedAt) {
        var bySkipReason = new LinkedHashMap<String, Long>();
        skipped.forEach((reason, count) -> bySkipReason.put(reason.label(), count));
        return new Snapshot(state, cutoverAt, new HistoryWindow(cutoverAt).start(), published, bySkipReason, malformed,
                completedAt);
    }

    private void persist(Snapshot snapshot) {
        persisted = true;
        if (file == null) {
            return;
        }
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            Files.writeString(file, JsonCodec.toJson(snapshot));
        } catch (IOException e) {
            LOG.warn("não foi possível gravar {}: {}", file, e.getMessage());
            throw new UncheckedIOException(e);
        }
    }

    private void load() {
        if (file == null || !Files.exists(file)) {
            return;
        }
        try {
            var saved = JsonCodec.fromJson(Files.readString(file), Snapshot.class);
            if ("COMPLETED".equals(saved.state()) && cutoverAt.equals(saved.cutoverAt())) {
                restored = saved;
                persisted = true;
            } else {
                LOG.info("ignorando {}: era de outro cutoverAt ({})", file, saved.cutoverAt());
            }
        } catch (IOException | RuntimeException e) {
            LOG.warn("ignorando {} ilegível: {}", file, e.getMessage());
        }
    }
}
