package br.com.motorfraude.transaction.infrastructure;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

public record BackfillSettings(Instant cutoverAt, int ratePerSecond, int statusPort, Duration completionQuiet) {

    static final int DEFAULT_RATE_PER_SECOND = 20_000;
    static final int DEFAULT_STATUS_PORT = 8096;
    static final int DEFAULT_QUIET_SECONDS = 10;

    public static BackfillSettings fromEnvironment(Map<String, String> env) {
        return new BackfillSettings(
                cutover(env.get("CUTOVER_AT")),
                positive(env, "BACKFILL_RATE_PER_SECOND", DEFAULT_RATE_PER_SECOND),
                positive(env, "STATUS_PORT", DEFAULT_STATUS_PORT),
                Duration.ofSeconds(positive(env, "BACKFILL_COMPLETION_QUIET_SECONDS", DEFAULT_QUIET_SECONDS)));
    }

    private static Instant cutover(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalStateException(
                    "CUTOVER_AT é obrigatório: o instante de corte em ISO-8601 UTC, por exemplo 2026-11-15T00:00:00Z");
        }
        try {
            return Instant.parse(text.trim());
        } catch (DateTimeParseException e) {
            throw new IllegalStateException(
                    "CUTOVER_AT inválido: '" + text + "'; use ISO-8601 UTC, por exemplo 2026-11-15T00:00:00Z", e);
        }
    }

    private static int positive(Map<String, String> env, String name, int fallback) {
        var text = env.get(name);
        if (text == null || text.isBlank()) {
            return fallback;
        }
        try {
            var value = Integer.parseInt(text.trim());
            if (value < 1) {
                throw new IllegalStateException(name + " deve ser 1 ou mais: " + text);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " inválido: '" + text + "'", e);
        }
    }
}
