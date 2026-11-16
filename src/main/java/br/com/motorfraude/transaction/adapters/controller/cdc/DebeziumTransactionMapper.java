package br.com.motorfraude.transaction.adapters.controller.cdc;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.transaction.application.usecase.TransactionChange;
import br.com.motorfraude.transaction.domain.HistoryRecord;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.Set;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

/**
 * Traduz o evento do Debezium da tabela {@code bank.transactions} nos campos que o histórico aceita. Só lê as colunas
 * permitidas: {@code payer_name} e {@code payer_cpf} nunca são acessadas.
 */
public final class DebeziumTransactionMapper {

    private static final Set<String> OPERATIONS = Set.of("c", "u", "r", "d");
    private static final String LAST_OF_SNAPSHOT = "last";

    private DebeziumTransactionMapper() {
    }

    public static Optional<TransactionChange> parse(String valueJson) {
        if (valueJson == null || valueJson.isBlank()) {
            return Optional.empty();
        }
        var event = tree(valueJson);
        var op = event.path("op").asString("");
        if (!OPERATIONS.contains(op)) {
            throw new CdcFormatException("op desconhecida no evento do CDC: '" + op + "'");
        }
        var lastOfSnapshot = LAST_OF_SNAPSHOT.equals(event.path("source").path("snapshot").asString(""));
        if ("d".equals(op)) {
            return Optional.of(new TransactionChange(op, lastOfSnapshot, null));
        }
        var after = event.path("after");
        if (!after.isObject()) {
            throw new CdcFormatException("campo after ausente no evento do CDC");
        }
        var record = new HistoryRecord(
                requiredText(after, "transaction_id"),
                requiredText(after, "account_token"),
                requiredLong(after, "amount_cents"),
                requiredText(after, "counterparty_token"),
                optionalText(after, "device_hash"),
                requiredInstant(after, "occurred_at"));
        return Optional.of(new TransactionChange(op, lastOfSnapshot, record));
    }

    private static JsonNode tree(String json) {
        try {
            return JsonCodec.readTree(json);
        } catch (JacksonException | IllegalArgumentException e) {
            throw new CdcFormatException("JSON do CDC inválido", e);
        }
    }

    private static String requiredText(JsonNode row, String field) {
        var value = row.path(field);
        if (!value.isString() || value.asString().isBlank()) {
            throw new CdcFormatException("campo " + field + " ausente no evento do CDC");
        }
        return value.asString();
    }

    private static String optionalText(JsonNode row, String field) {
        var value = row.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        return requiredText(row, field);
    }

    private static long requiredLong(JsonNode row, String field) {
        var value = row.path(field);
        if (!value.isIntegralNumber()) {
            throw new CdcFormatException("campo " + field + " ausente ou não numérico no evento do CDC");
        }
        return value.asLong();
    }

    private static Instant requiredInstant(JsonNode row, String field) {
        var text = requiredText(row, field);
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            throw new CdcFormatException("campo " + field + " com data inválida: " + text, e);
        }
    }
}
