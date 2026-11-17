package br.com.motorfraude.transaction.application.usecase;

import br.com.motorfraude.transaction.domain.HistoryRecord;

public record TransactionChange(String op, boolean lastOfSnapshot, HistoryRecord record) {
}
