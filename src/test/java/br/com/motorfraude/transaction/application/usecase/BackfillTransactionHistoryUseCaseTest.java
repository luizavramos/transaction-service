package br.com.motorfraude.transaction.application.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.transaction.domain.HistoryRecord;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BackfillTransactionHistoryUseCaseTest {

    private static final Instant CUTOVER = Instant.parse("2026-11-15T00:00:00Z");

    private final BackfillTransactionHistoryUseCase useCase =
            new BackfillTransactionHistoryUseCase(new HistoryWindow(CUTOVER));

    private static HistoryRecord record(Instant occurredAt) {
        return new HistoryRecord("txn_1", "acc_1", 1500L, "acc_2", "dev_1", occurredAt);
    }

    private static TransactionChange change(String op, Instant occurredAt) {
        return new TransactionChange(op, false, record(occurredAt));
    }

    @Test
    @DisplayName("leitura do snapshot dentro da janela vira registro do histórico")
    void dentroDaJanela() {
        var outcome = useCase.evaluate(change("r", CUTOVER.minus(Duration.ofDays(10))));

        assertThat(outcome.published()).isTrue();
        assertThat(outcome.record()).isEqualTo(record(CUTOVER.minus(Duration.ofDays(10))));
        assertThat(outcome.skipReason()).isNull();
    }

    @Test
    @DisplayName("leitura do snapshot fora da janela é descartada, com o motivo")
    void foraDaJanela() {
        var antiga = useCase.evaluate(change("r", CUTOVER.minus(Duration.ofDays(91))));
        var futura = useCase.evaluate(change("r", CUTOVER));

        assertThat(antiga.published()).isFalse();
        assertThat(antiga.skipReason()).isEqualTo(SkipReason.OUT_OF_WINDOW);
        assertThat(futura.skipReason()).isEqualTo(SkipReason.OUT_OF_WINDOW);
    }

    @Test
    @DisplayName("as bordas da janela: 90 dias exatos entra, o cutoverAt não")
    void bordas() {
        assertThat(useCase.evaluate(change("r", CUTOVER.minus(Duration.ofDays(90)))).published()).isTrue();
        assertThat(useCase.evaluate(change("r", CUTOVER)).published()).isFalse();
    }

    @Test
    @DisplayName("valor zero ou negativo no legado é registro inválido: não vira mensagem fora do contrato")
    void valorInvalido() {
        var zero = new TransactionChange("r", false,
                new HistoryRecord("txn_1", "acc_1", 0L, "acc_2", "dev_1", CUTOVER.minus(Duration.ofDays(1))));
        var negativo = new TransactionChange("r", false,
                new HistoryRecord("txn_1", "acc_1", -10L, "acc_2", "dev_1", CUTOVER.minus(Duration.ofDays(1))));

        assertThat(useCase.evaluate(zero).skipReason()).isEqualTo(SkipReason.INVALID_RECORD);
        assertThat(useCase.evaluate(negativo).skipReason()).isEqualTo(SkipReason.INVALID_RECORD);
    }

    @Test
    @DisplayName("c, u e d não são backfill: o histórico só vem do snapshot; a mudança em dia chega pelo fluxo normal")
    void ignoraOutrasOperacoes() {
        for (var op : new String[] {"c", "u"}) {
            var outcome = useCase.evaluate(change(op, CUTOVER.minus(Duration.ofDays(1))));
            assertThat(outcome.published()).isFalse();
            assertThat(outcome.skipReason()).isEqualTo(SkipReason.NOT_SNAPSHOT);
        }
        var removal = useCase.evaluate(new TransactionChange("d", false, null));
        assertThat(removal.skipReason()).isEqualTo(SkipReason.NOT_SNAPSHOT);
    }
}
