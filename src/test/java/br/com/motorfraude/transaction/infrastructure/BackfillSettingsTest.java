package br.com.motorfraude.transaction.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BackfillSettingsTest {

    @Test
    @DisplayName("CUTOVER_AT em ISO-8601 UTC; os demais valores têm padrão")
    void valido() {
        var settings = BackfillSettings.fromEnvironment(Map.of("CUTOVER_AT", "2026-11-15T00:00:00Z"));

        assertThat(settings.cutoverAt()).isEqualTo(Instant.parse("2026-11-15T00:00:00Z"));
        assertThat(settings.ratePerSecond()).isEqualTo(20_000);
        assertThat(settings.statusPort()).isEqualTo(8096);
        assertThat(settings.completionQuiet()).isEqualTo(java.time.Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("taxa e porta do status podem ser trocadas")
    void sobrescreve() {
        var settings = BackfillSettings.fromEnvironment(
                Map.of("CUTOVER_AT", "2026-11-15T00:00:00Z", "BACKFILL_RATE_PER_SECOND", "5000", "STATUS_PORT", "9000"));

        assertThat(settings.ratePerSecond()).isEqualTo(5000);
        assertThat(settings.statusPort()).isEqualTo(9000);
    }

    @Test
    @DisplayName("sem CUTOVER_AT a partida é recusada, dizendo o que falta")
    void ausente() {
        assertThatThrownBy(() -> BackfillSettings.fromEnvironment(Map.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CUTOVER_AT");
        assertThatThrownBy(() -> BackfillSettings.fromEnvironment(Map.of("CUTOVER_AT", "  ")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CUTOVER_AT");
    }

    @Test
    @DisplayName("CUTOVER_AT inválido, ou sem fuso, é recusado")
    void invalido() {
        assertThatThrownBy(() -> BackfillSettings.fromEnvironment(Map.of("CUTOVER_AT", "amanhã")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CUTOVER_AT");
        assertThatThrownBy(() -> BackfillSettings.fromEnvironment(Map.of("CUTOVER_AT", "2026-11-15T00:00:00")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CUTOVER_AT");
    }

    @Test
    @DisplayName("taxa inválida é recusada")
    void taxaInvalida() {
        assertThatThrownBy(() -> BackfillSettings.fromEnvironment(
                Map.of("CUTOVER_AT", "2026-11-15T00:00:00Z", "BACKFILL_RATE_PER_SECOND", "0")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("BACKFILL_RATE_PER_SECOND");
    }
}
