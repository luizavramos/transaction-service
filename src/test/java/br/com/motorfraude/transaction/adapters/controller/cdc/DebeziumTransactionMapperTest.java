package br.com.motorfraude.transaction.adapters.controller.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DebeziumTransactionMapperTest {

    static String fixture(String name) {
        try (var in = DebeziumTransactionMapperTest.class.getResourceAsStream("/debezium/" + name)) {
            if (in == null) {
                throw new IllegalStateException("fixture não encontrada: " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @DisplayName("leitura do snapshot (r): vira o registro só com os campos permitidos")
    void snapshot() {
        var change = DebeziumTransactionMapper.parse(fixture("transaction-r.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("r");
        assertThat(change.lastOfSnapshot()).isFalse();
        var record = change.record();
        assertThat(record.transactionId()).isEqualTo("txn_0000000001");
        assertThat(record.accountId()).isEqualTo("acc_tok_00000002");
        assertThat(record.counterpartyToken()).isEqualTo("acc_tok_00000011");
        assertThat(record.amountCents()).isEqualTo(8919L);
        assertThat(record.deviceHash()).isEqualTo("dev_a8ff82c8aed98ffd");
        assertThat(record.occurredAt()).isEqualTo(Instant.parse("2026-11-13T18:54:31Z"));
    }

    @Test
    @DisplayName("nome e CPF do pagador não chegam ao registro, em campo nenhum")
    void semDadoPessoal() {
        var change = DebeziumTransactionMapper.parse(fixture("transaction-r.json")).orElseThrow();

        assertThat(change.record().toString())
                .doesNotContain("Pagador")
                .doesNotContain("001.001.001-01")
                .doesNotContain("payer");
        assertThat(change.record().getClass().getRecordComponents())
                .extracting(c -> c.getName().toLowerCase())
                .noneMatch(name -> name.contains("payer") || name.contains("cpf") || name.contains("name"));
    }

    @Test
    @DisplayName("o último evento do snapshot (snapshot=last) é marcado, e ainda traz o registro")
    void ultimoDoSnapshot() {
        var change = DebeziumTransactionMapper.parse(fixture("transaction-r-last.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("r");
        assertThat(change.lastOfSnapshot()).isTrue();
        assertThat(change.record().transactionId()).isEqualTo("txn_0000000002");
    }

    @Test
    @DisplayName("criação (c) e atualização (u) são mapeadas, mas sem marca de snapshot")
    void criacaoEAtualizacao() {
        var created = DebeziumTransactionMapper.parse(fixture("transaction-c.json")).orElseThrow();
        var updated = DebeziumTransactionMapper.parse(fixture("transaction-u.json")).orElseThrow();

        assertThat(created.op()).isEqualTo("c");
        assertThat(created.lastOfSnapshot()).isFalse();
        assertThat(created.record().amountCents()).isEqualTo(24757L);
        assertThat(updated.op()).isEqualTo("u");
        assertThat(updated.record().amountCents()).isEqualTo(24758L);
    }

    @Test
    @DisplayName("remoção (d): sem registro, porque o before só tem a chave e valores zerados")
    void remocao() {
        var change = DebeziumTransactionMapper.parse(fixture("transaction-d.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("d");
        assertThat(change.record()).isNull();
    }

    @Test
    @DisplayName("o tombstone do Debezium (valor nulo ou vazio) não gera mudança")
    void tombstone() {
        assertThat(DebeziumTransactionMapper.parse(null)).isEmpty();
        assertThat(DebeziumTransactionMapper.parse("  ")).isEmpty();
    }

    @Test
    @DisplayName("device_hash nulo é aceito: o registro sai sem dispositivo")
    void semDispositivo() {
        var json = fixture("transaction-r.json").replace("\"device_hash\":\"dev_a8ff82c8aed98ffd\"", "\"device_hash\":null");

        var change = DebeziumTransactionMapper.parse(json).orElseThrow();

        assertThat(change.record().deviceHash()).isNull();
    }

    @Test
    @DisplayName("JSON inválido ou sem campo obrigatório é rejeitado, dizendo qual")
    void invalido() {
        assertThatThrownBy(() -> DebeziumTransactionMapper.parse("{não é json"))
                .isInstanceOf(CdcFormatException.class);
        assertThatThrownBy(() -> DebeziumTransactionMapper.parse("{\"op\":\"x\",\"ts_ms\":1}"))
                .isInstanceOf(CdcFormatException.class).hasMessageContaining("op");
        assertThatThrownBy(() -> DebeziumTransactionMapper.parse(
                fixture("transaction-r.json").replace("\"account_token\":\"acc_tok_00000002\",", "")))
                .isInstanceOf(CdcFormatException.class).hasMessageContaining("account_token");
        assertThatThrownBy(() -> DebeziumTransactionMapper.parse(
                fixture("transaction-r.json").replace("\"amount_cents\":8919", "\"amount_cents\":\"abc\"")))
                .isInstanceOf(CdcFormatException.class).hasMessageContaining("amount_cents");
        assertThatThrownBy(() -> DebeziumTransactionMapper.parse(
                fixture("transaction-r.json").replace("2026-11-13T18:54:31.000000Z", "ontem")))
                .isInstanceOf(CdcFormatException.class).hasMessageContaining("occurred_at");
    }
}
