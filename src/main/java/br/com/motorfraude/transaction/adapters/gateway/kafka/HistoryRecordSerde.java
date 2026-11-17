package br.com.motorfraude.transaction.adapters.gateway.kafka;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Money;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.transaction.domain.HistoryRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class HistoryRecordSerde implements Serde<HistoryRecord> {

    private static final String CURRENCY = "BRL";

    @Override
    public Serializer<HistoryRecord> serializer() {
        return (topic, record) -> record == null ? null : JsonCodec.toBytes(toMessage(record));
    }

    @Override
    public Deserializer<HistoryRecord> deserializer() {
        return (topic, bytes) -> bytes == null ? null : fromMessage(JsonCodec.fromJson(bytes, TransactionHistoryRecord.class));
    }

    private static TransactionHistoryRecord toMessage(HistoryRecord record) {
        return new TransactionHistoryRecord(
                record.transactionId(),
                record.accountId(),
                new Money(record.amountCents(), CURRENCY),
                record.counterpartyToken(),
                record.deviceHash(),
                record.occurredAt());
    }

    private static HistoryRecord fromMessage(TransactionHistoryRecord message) {
        return new HistoryRecord(
                message.transactionId(),
                message.accountId(),
                message.amount().value(),
                message.counterpartyToken(),
                message.deviceIdHash(),
                message.occurredAt());
    }
}
