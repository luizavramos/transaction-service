package br.com.motorfraude.transaction.adapters.gateway.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.transaction.application.usecase.SkipReason;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MicrometerBackfillMetricsTest {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    @Test
    @DisplayName("as séries existem em zero antes do primeiro evento: um painel não vê buraco")
    void zeroNaPartida() {
        new MicrometerBackfillMetrics(registry);

        assertThat(registry.scrape())
                .contains("backfill_records_published_total 0.0")
                .contains("backfill_records_skipped_total{reason=\"not_snapshot\"} 0.0")
                .contains("backfill_records_skipped_total{reason=\"out_of_window\"} 0.0")
                .contains("backfill_records_skipped_total{reason=\"invalid_record\"} 0.0")
                .contains("backfill_events_malformed_total 0.0");
    }

    @Test
    @DisplayName("conta publicados, descartados por motivo e malformados")
    void conta() {
        var metrics = new MicrometerBackfillMetrics(registry);

        metrics.published();
        metrics.published();
        metrics.skipped(SkipReason.OUT_OF_WINDOW);
        metrics.malformed();

        assertThat(registry.scrape())
                .contains("backfill_records_published_total 2.0")
                .contains("backfill_records_skipped_total{reason=\"out_of_window\"} 1.0")
                .contains("backfill_events_malformed_total 1.0");
    }
}
