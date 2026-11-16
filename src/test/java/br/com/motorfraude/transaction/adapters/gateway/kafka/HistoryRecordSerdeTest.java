package br.com.motorfraude.transaction.adapters.gateway.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.transaction.domain.HistoryRecord;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HistoryRecordSerdeTest {

    private static final HistoryRecord RECORD = new HistoryRecord(
            "txn_0000000001", "acc_tok_00000002", 8919L, "acc_tok_00000011", "dev_a8ff82c8aed98ffd",
            Instant.parse("2026-11-13T18:54:31Z"));

    @Test
    @DisplayName("serializa no contrato transaction-history-record.v1, valor em centavos e moeda BRL")
    void contrato() {
        var bytes = new HistoryRecordSerde().serializer().serialize("t", RECORD);

        ContractAssertions.assertProduces("transaction-history-record.v1", bytes);
        var message = JsonCodec.fromJson(bytes, TransactionHistoryRecord.class);
        assertThat(message.transactionId()).isEqualTo("txn_0000000001");
        assertThat(message.accountId()).isEqualTo("acc_tok_00000002");
        assertThat(message.amount().value()).isEqualTo(8919L);
        assertThat(message.amount().currency()).isEqualTo("BRL");
        assertThat(message.counterpartyToken()).isEqualTo("acc_tok_00000011");
        assertThat(message.deviceIdHash()).isEqualTo("dev_a8ff82c8aed98ffd");
        assertThat(message.occurredAt()).isEqualTo(Instant.parse("2026-11-13T18:54:31Z"));
    }

    @Test
    @DisplayName("sem dispositivo, o campo some do JSON (o contrato não aceita nulo)")
    void semDispositivo() {
        var noDevice = new HistoryRecord("txn_0000000001", "acc_tok_00000002", 100L, "acc_tok_00000011", null,
                Instant.parse("2026-11-13T18:54:31Z"));

        var json = new String(new HistoryRecordSerde().serializer().serialize("t", noDevice), StandardCharsets.UTF_8);

        ContractAssertions.assertProduces("transaction-history-record.v1", json);
        assertThat(json).doesNotContain("deviceIdHash");
    }

    @Test
    @DisplayName("ida e volta devolve o mesmo registro; nulo fica nulo")
    void idaEVolta() {
        var serde = new HistoryRecordSerde();

        var bytes = serde.serializer().serialize("t", RECORD);

        assertThat(serde.deserializer().deserialize("t", bytes)).isEqualTo(RECORD);
        assertThat(serde.serializer().serialize("t", null)).isNull();
        assertThat(serde.deserializer().deserialize("t", null)).isNull();
    }
}
