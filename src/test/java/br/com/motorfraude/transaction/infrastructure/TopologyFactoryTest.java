package br.com.motorfraude.transaction.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.transaction.adapters.gateway.micrometer.MicrometerBackfillMetrics;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopologyFactoryTest {

    private static final Instant CUTOVER = Instant.parse("2026-11-15T00:00:00Z");

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> output;
    private BackfillStatus status;
    private PrometheusMeterRegistry registry;
    private int sequence;

    @BeforeEach
    void setUp() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        status = new BackfillStatus(CUTOVER, Clock.systemUTC(), null);
        var topology = TopologyFactory.create(new HistoryWindow(CUTOVER), new MicrometerBackfillMetrics(registry), status,
                new RateLimiter(1_000_000, 1_000_000));
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-service-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(TopologyFactory.SOURCE_TOPIC, new StringSerializer(), new StringSerializer());
        output = driver.createOutputTopic(TopologyFactory.BACKFILL_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        registry.close();
    }

    private String event(String op, String snapshot, String account, long amountCents, Instant occurredAt) {
        var id = "txn_%010d".formatted(++sequence);
        var row = """
                {"transaction_id":"%s","account_token":"%s","counterparty_token":"acc_tok_00000099","amount_cents":%d,
                 "device_hash":"dev_a8ff82c8aed98ffd","occurred_at":"%s","payer_name":"Fulano Pagador","payer_cpf":"111.222.333-96"}
                """.formatted(id, account, amountCents, occurredAt);
        var after = "d".equals(op) ? "null" : row;
        return "{\"before\":null,\"after\":%s,\"source\":{\"snapshot\":\"%s\"},\"op\":\"%s\",\"ts_ms\":1794789053054}"
                .formatted(after, snapshot, op);
    }

    private void snapshotRow(String account, Instant occurredAt) {
        input.pipeInput("{\"transaction_id\":\"k\"}", event("r", "true", account, 1500, occurredAt));
    }

    private List<TestRecord<String, String>> drain() {
        var all = new ArrayList<TestRecord<String, String>>();
        while (!output.isEmpty()) {
            all.add(output.readRecord());
        }
        return all;
    }

    @Test
    @DisplayName("leitura do snapshot na janela vira registro do contrato, com chave = accountId")
    void publicaNaJanela() {
        snapshotRow("acc_tok_00000001", CUTOVER.minus(Duration.ofDays(10)));

        var records = drain();

        assertThat(records).hasSize(1);
        assertThat(records.getFirst().key()).isEqualTo("acc_tok_00000001");
        ContractAssertions.assertProduces("transaction-history-record.v1", records.getFirst().value());
        var message = JsonCodec.fromJson(records.getFirst().value(), TransactionHistoryRecord.class);
        assertThat(message.accountId()).isEqualTo("acc_tok_00000001");
        assertThat(message.amount().value()).isEqualTo(1500L);
        assertThat(status.snapshot().published()).isEqualTo(1);
    }

    @Test
    @DisplayName("nada do pagador chega ao tópico: nem nome nem CPF")
    void semDadoPessoal() {
        snapshotRow("acc_tok_00000001", CUTOVER.minus(Duration.ofDays(1)));

        var value = drain().getFirst().value();

        assertThat(value).doesNotContain("Fulano").doesNotContain("111.222.333-96").doesNotContain("payer");
    }

    @Test
    @DisplayName("as bordas da janela: 90 dias exatos entra; um milissegundo antes, o cutoverAt e depois dele não")
    void bordasDaJanela() {
        snapshotRow("acc_tok_00000001", CUTOVER.minus(Duration.ofDays(90)));
        snapshotRow("acc_tok_00000002", CUTOVER.minus(Duration.ofDays(90)).minusMillis(1));
        snapshotRow("acc_tok_00000003", CUTOVER.minusMillis(1));
        snapshotRow("acc_tok_00000004", CUTOVER);
        snapshotRow("acc_tok_00000005", CUTOVER.plusSeconds(60));

        var keys = drain().stream().map(TestRecord::key).toList();

        assertThat(keys).containsExactly("acc_tok_00000001", "acc_tok_00000003");
        var snapshot = status.snapshot();
        assertThat(snapshot.published()).isEqualTo(2);
        assertThat(snapshot.skipped()).containsEntry("out_of_window", 3L);
    }

    @Test
    @DisplayName("c, u e d não geram registro: depois do snapshot o histórico não é refeito pelo backfill")
    void ignoraMudancasDepoisDoSnapshot() {
        var at = CUTOVER.minus(Duration.ofDays(1));
        input.pipeInput("{\"transaction_id\":\"k\"}", event("c", "false", "acc_tok_00000001", 1500, at));
        input.pipeInput("{\"transaction_id\":\"k\"}", event("u", "false", "acc_tok_00000001", 1600, at));
        input.pipeInput("{\"transaction_id\":\"k\"}", event("d", "false", "acc_tok_00000001", 0, at));

        assertThat(drain()).isEmpty();
        assertThat(status.snapshot().skipped()).containsEntry("not_snapshot", 3L);
    }

    @Test
    @DisplayName("o tombstone do Debezium é ignorado sem contar como descarte")
    void tombstone() {
        input.pipeInput("{\"transaction_id\":\"k\"}", (String) null);

        assertThat(drain()).isEmpty();
        assertThat(status.snapshot().skipped().values()).containsOnly(0L);
        assertThat(status.snapshot().malformed()).isZero();
    }

    @Test
    @DisplayName("evento malformado é descartado e contado, e o seguinte segue normal")
    void malformado() {
        input.pipeInput("{\"transaction_id\":\"k\"}", "{não é json");
        snapshotRow("acc_tok_00000001", CUTOVER.minus(Duration.ofDays(2)));

        assertThat(drain()).hasSize(1);
        assertThat(status.snapshot().malformed()).isEqualTo(1);
    }

    @Test
    @DisplayName("valor zero ou negativo no legado não vira mensagem fora do contrato: é descartado como registro inválido")
    void valorInvalido() {
        input.pipeInput("{\"transaction_id\":\"k\"}",
                event("r", "true", "acc_tok_00000001", 0, CUTOVER.minus(Duration.ofDays(2))));
        input.pipeInput("{\"transaction_id\":\"k\"}",
                event("r", "true", "acc_tok_00000001", -5, CUTOVER.minus(Duration.ofDays(2))));

        assertThat(drain()).isEmpty();
        assertThat(status.snapshot().skipped()).containsEntry("invalid_record", 2L);
    }

    @Test
    @DisplayName("snapshot=last vira COMPLETED; antes dele o estado é RUNNING")
    void completa() {
        snapshotRow("acc_tok_00000001", CUTOVER.minus(Duration.ofDays(2)));
        assertThat(status.snapshot().state()).isEqualTo("RUNNING");

        input.pipeInput("{\"transaction_id\":\"k\"}",
                event("r", "last", "acc_tok_00000002", 1500, CUTOVER.minus(Duration.ofDays(3))));

        assertThat(status.snapshot().state()).isEqualTo("COMPLETED");
        assertThat(status.snapshot().published()).isEqualTo(2);
        assertThat(drain()).hasSize(2);
    }

    @Test
    @DisplayName("o último evento do snapshot fora da janela também completa (a conta do fim vem antes do filtro)")
    void completaMesmoFora() {
        input.pipeInput("{\"transaction_id\":\"k\"}",
                event("r", "last", "acc_tok_00000001", 1500, CUTOVER.minus(Duration.ofDays(200))));

        assertThat(drain()).isEmpty();
        assertThat(status.snapshot().state()).isEqualTo("COMPLETED");
        assertThat(status.snapshot().skipped()).containsEntry("out_of_window", 1L);
    }

    @Test
    @DisplayName("as métricas contam publicados e descartados por motivo")
    void metricas() {
        snapshotRow("acc_tok_00000001", CUTOVER.minus(Duration.ofDays(2)));
        snapshotRow("acc_tok_00000002", CUTOVER.plusSeconds(1));

        var scrape = registry.scrape();

        assertThat(scrape).contains("backfill_records_published_total 1.0")
                .contains("backfill_records_skipped_total{reason=\"out_of_window\"} 1.0");
    }
}
