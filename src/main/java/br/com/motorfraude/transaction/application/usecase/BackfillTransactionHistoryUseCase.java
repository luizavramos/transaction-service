package br.com.motorfraude.transaction.application.usecase;

import br.com.motorfraude.transaction.domain.HistoryRecord;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import java.util.Objects;

/** Decide se um evento do CDC vira registro de histórico: só leitura do snapshot dentro da janela. */
public final class BackfillTransactionHistoryUseCase {

    public record Outcome(HistoryRecord record, SkipReason skipReason) {

        public boolean published() {
            return record != null;
        }
    }

    private final HistoryWindow window;

    public BackfillTransactionHistoryUseCase(HistoryWindow window) {
        this.window = Objects.requireNonNull(window, "window");
    }

    public Outcome evaluate(TransactionChange change) {
        if (!"r".equals(change.op()) || change.record() == null) {
            return new Outcome(null, SkipReason.NOT_SNAPSHOT);
        }
        if (change.record().amountCents() <= 0) {
            return new Outcome(null, SkipReason.INVALID_RECORD);
        }
        if (!window.contains(change.record().occurredAt())) {
            return new Outcome(null, SkipReason.OUT_OF_WINDOW);
        }
        return new Outcome(change.record(), null);
    }
}
