package br.com.motorfraude.transaction.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HistoryWindowTest {

    private static final Instant CUTOVER = Instant.parse("2026-11-15T00:00:00Z");
    private static final Instant START = CUTOVER.minus(Duration.ofDays(90));

    private final HistoryWindow window = new HistoryWindow(CUTOVER);

    @Test
    @DisplayName("a janela vai de cutoverAt − 90 dias (incluído) até cutoverAt (excluído)")
    void limites() {
        assertThat(window.start()).isEqualTo(Instant.parse("2026-08-17T00:00:00Z"));
        assertThat(window.start()).isEqualTo(START);
        assertThat(window.end()).isEqualTo(CUTOVER);
    }

    @Test
    @DisplayName("exatamente 90 dias antes do corte entra")
    void exatamenteNoveDias() {
        assertThat(window.contains(START)).isTrue();
    }

    @Test
    @DisplayName("1 milissegundo antes dos 90 dias fica de fora")
    void umMilissegundoAntes() {
        assertThat(window.contains(START.minusMillis(1))).isFalse();
    }

    @Test
    @DisplayName("exatamente no cutoverAt fica de fora: o que vem dele em diante chega pelo fluxo normal")
    void cutoverExcluido() {
        assertThat(window.contains(CUTOVER)).isFalse();
    }

    @Test
    @DisplayName("1 milissegundo antes do corte entra")
    void umMilissegundoAntesDoCorte() {
        assertThat(window.contains(CUTOVER.minusMillis(1))).isTrue();
    }

    @Test
    @DisplayName("depois do corte e muito antes da janela ficam de fora")
    void foraDaJanela() {
        assertThat(window.contains(CUTOVER.plusSeconds(1))).isFalse();
        assertThat(window.contains(CUTOVER.minus(Duration.ofDays(120)))).isFalse();
    }

    @Test
    @DisplayName("no meio da janela entra")
    void meio() {
        assertThat(window.contains(CUTOVER.minus(Duration.ofDays(45)))).isTrue();
    }

    @Test
    @DisplayName("sem cutoverAt a janela não existe")
    void semCorte() {
        assertThatThrownBy(() -> new HistoryWindow(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("occurredAt nulo não pertence a janela nenhuma")
    void occurredAtNulo() {
        assertThatThrownBy(() -> window.contains(null)).isInstanceOf(NullPointerException.class);
    }
}
