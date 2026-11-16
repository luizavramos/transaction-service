package br.com.motorfraude.transaction.application.usecase;

import br.com.motorfraude.transaction.domain.HistoryRecord;

/**
 * Evento do CDC da tabela de transações, já mapeado.
 *
 * @param op               operação do Debezium: r (leitura do snapshot), c, u ou d
 * @param lastOfSnapshot   true no último evento do snapshot ({@code source.snapshot=last})
 * @param record           a linha com só os campos permitidos; nulo em d
 */
public record TransactionChange(String op, boolean lastOfSnapshot, HistoryRecord record) {
}
