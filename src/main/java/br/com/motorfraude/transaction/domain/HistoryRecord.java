package br.com.motorfraude.transaction.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Transação do histórico, só com os campos que o motor precisa. Não há campo para nome nem CPF do pagador:
 * o que não existe aqui não tem como vazar.
 */
public record HistoryRecord(
        String transactionId,
        String accountId,
        long amountCents,
        String counterpartyToken,
        String deviceHash,
        Instant occurredAt) {

    public HistoryRecord {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(counterpartyToken, "counterpartyToken");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }
}
