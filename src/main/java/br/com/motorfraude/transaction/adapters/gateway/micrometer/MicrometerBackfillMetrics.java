package br.com.motorfraude.transaction.adapters.gateway.micrometer;

import br.com.motorfraude.transaction.application.gateway.BackfillMetrics;
import br.com.motorfraude.transaction.application.usecase.SkipReason;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.EnumMap;
import java.util.Map;

public final class MicrometerBackfillMetrics implements BackfillMetrics {

    private final Counter published;
    private final Counter malformed;
    private final Map<SkipReason, Counter> skipped = new EnumMap<>(SkipReason.class);

    public MicrometerBackfillMetrics(MeterRegistry registry) {
        this.published = Counter.builder("backfill.records.published")
                .description("Registros de histórico publicados em transaction-history-backfill.v1")
                .register(registry);
        this.malformed = Counter.builder("backfill.events.malformed")
                .description("Eventos do CDC descartados por não terem como ser lidos")
                .register(registry);
        for (var reason : SkipReason.values()) {
            skipped.put(reason, Counter.builder("backfill.records.skipped")
                    .description("Eventos do CDC lidos e não publicados, por motivo")
                    .tag("reason", reason.label())
                    .register(registry));
        }
    }

    @Override
    public void published() {
        published.increment();
    }

    @Override
    public void skipped(SkipReason reason) {
        skipped.get(reason).increment();
    }

    @Override
    public void malformed() {
        malformed.increment();
    }
}
