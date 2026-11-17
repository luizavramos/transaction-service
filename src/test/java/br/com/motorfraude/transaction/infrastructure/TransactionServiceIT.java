package br.com.motorfraude.transaction.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;
import br.com.motorfraude.transaction.adapters.controller.rest.StatusServer;
import br.com.motorfraude.transaction.adapters.gateway.micrometer.MicrometerBackfillMetrics;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class TransactionServiceIT {

    private static final Instant CUTOVER = Instant.parse("2026-11-15T00:00:00Z");
    private static final int ROWS = 10_000;
    private static final int REFERENCE_ACCOUNTS = 100;
    private static final String CUTOVER_SQL = "timestamptz '2026-11-15T00:00:00Z'";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16.16")
            .withNetwork(NETWORK).withNetworkAliases("bank-db")
            .withDatabaseName("bank").withUsername("bank").withPassword("bank")
            .withInitScript("bank-schema.sql")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withNetwork(NETWORK).withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.6.3.Final")
            .withNetwork(NETWORK)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:9093")
            .withEnv("GROUP_ID", "connect-it")
            .withEnv("CONFIG_STORAGE_TOPIC", "connect_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "connect_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "connect_statuses")
            .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .waitingFor(Wait.forHttp("/connectors").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

    private static final String CONNECTOR = """
            {"connector.class":"io.debezium.connector.postgresql.PostgresConnector","tasks.max":"1",
             "database.hostname":"bank-db","database.port":"5432","database.user":"bank","database.password":"bank","database.dbname":"bank",
             "topic.prefix":"cdc","plugin.name":"pgoutput","slot.name":"transactions_cdc_slot","publication.name":"dbz_transactions",
             "publication.autocreate.mode":"disabled","table.include.list":"bank.transactions",
             "snapshot.mode":"initial","tombstones.on.delete":"true",
             "key.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":"false",
             "value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"false"}
            """;

    private static final Pattern CPF = Pattern.compile("\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}");

    private static void sql(String statement) throws Exception {
        try (Connection c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), "bank", "bank"); var s = c.createStatement()) {
            s.execute(statement);
        }
    }

    private static List<String[]> query(String statement) throws Exception {
        var rows = new ArrayList<String[]>();
        try (Connection c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), "bank", "bank");
                var s = c.createStatement();
                var rs = s.executeQuery(statement)) {
            var columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                var row = new String[columns];
                for (int i = 0; i < columns; i++) {
                    row[i] = rs.getString(i + 1);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static String connectUrl(String path) {
        return "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + path;
    }

    private static void registerConnector() throws Exception {
        var client = HttpClient.newHttpClient();
        var put = HttpRequest.newBuilder(URI.create(connectUrl("/connectors/transactions-cdc/config")))
                .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(CONNECTOR)).build();
        var response = client.send(put, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isIn(200, 201);
        for (int i = 0; i < 60; i++) {
            var status = client.send(HttpRequest.newBuilder(URI.create(connectUrl("/connectors/transactions-cdc/status"))).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            if (status.contains("\"state\":\"RUNNING\"") && status.contains("\"tasks\":[{")) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("conector não chegou a RUNNING");
    }

    private static String get(String url) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(url)).build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    private static void seed() throws Exception {
        sql("""
                INSERT INTO bank.transactions
                SELECT 'txn_' || lpad(n::text, 10, '0'),
                       'acc_tok_' || lpad((CASE WHEN n <= 2000 THEN (n %% %d) + 1 ELSE %d + (n %% 900) + 1 END)::text, 8, '0'),
                       'acc_tok_' || lpad((((n * 7 + 3) %% 1000) + 1)::text, 8, '0'),
                       1000 + (n * 7919) %% 500000,
                       'dev_' || substr(md5(n::text), 1, 16),
                       %s - (((n * 104729) %% (130 * 86400)) - 10 * 86400) * interval '1 second',
                       'Pagador Sintetico ' || n,
                       lpad((n %% 1000)::text, 3, '0') || '.' || lpad((n %% 997)::text, 3, '0') || '.' || lpad((n %% 991)::text, 3, '0')
                           || '-' || lpad((n %% 100)::text, 2, '0')
                FROM generate_series(1::bigint, %d) AS n
                """.formatted(REFERENCE_ACCOUNTS, REFERENCE_ACCOUNTS, CUTOVER_SQL, ROWS - 4));
        sql("INSERT INTO bank.transactions VALUES "
                + "('txn_edge_0001','acc_tok_00000001','acc_tok_00000500',5000,'dev_edge0000000001'," + CUTOVER_SQL + " - 90 * 86400 * interval '1 second','Borda Um','111.111.111-11'),"
                + "('txn_edge_0002','acc_tok_00000001','acc_tok_00000500',5000,'dev_edge0000000001'," + CUTOVER_SQL + " - (90 * 86400 + 1) * interval '1 second','Borda Dois','222.222.222-22'),"
                + "('txn_edge_0003','acc_tok_00000001','acc_tok_00000500',5000,'dev_edge0000000001'," + CUTOVER_SQL + " - interval '1 second','Borda Tres','333.333.333-33'),"
                + "('txn_edge_0004','acc_tok_00000001','acc_tok_00000500',5000,'dev_edge0000000001'," + CUTOVER_SQL + ",'Borda Quatro','444.444.444-44')");
    }

    private static String windowPredicate() {
        return "occurred_at >= " + CUTOVER_SQL + " - 90 * 86400 * interval '1 second' AND occurred_at < " + CUTOVER_SQL;
    }

    private static List<ConsumerRecord<String, String>> readAll(int expected, Duration timeout) {
        var all = new ArrayList<ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<String, String>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(TopologyFactory.BACKFILL_TOPIC));
            var deadline = System.nanoTime() + timeout.toNanos();
            while (all.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(all::add);
            }
            var extra = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < extra) {
                consumer.poll(Duration.ofMillis(200)).forEach(all::add);
            }
        }
        return all;
    }

    @Test
    @DisplayName("10 mil transações do banco ao tópico: só a janela de 90 dias, sem dado pessoal (SC-002), status COMPLETED")
    void ponta_a_ponta(@TempDir Path tmp) throws Exception {
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TopologyFactory.SOURCE_TOPIC, 3, (short) 1),
                    new NewTopic(TopologyFactory.BACKFILL_TOPIC, 6, (short) 1))).all().get();
        }
        seed();
        var total = Integer.parseInt(query("SELECT count(*) FROM bank.transactions").getFirst()[0]);
        var expectedIds = new TreeSet<String>();
        query("SELECT transaction_id FROM bank.transactions WHERE " + windowPredicate()).forEach(r -> expectedIds.add(r[0]));
        assertThat(total).isEqualTo(ROWS);
        assertThat(expectedIds).as("a janela precisa ter coisa dentro e coisa fora").hasSizeBetween(1, ROWS - 1)
                .contains("txn_edge_0001", "txn_edge_0003").doesNotContain("txn_edge_0002", "txn_edge_0004");

        registerConnector();

        var settings = new StreamsSettings("transaction-service-it", KAFKA.getBootstrapServers(), tmp, 1, 0, "transaction-service-it-0", 0);
        var status = new BackfillStatus(CUTOVER, Clock.systemUTC(), tmp.resolve("backfill-status.json"), Duration.ofSeconds(3));
        try (var app = StreamsAppBootstrap.create(settings, registry -> TopologyFactory.create(new HistoryWindow(CUTOVER),
                        new MicrometerBackfillMetrics(registry), status, new RateLimiter(1_000_000, 100_000)));
                var statusServer = new StatusServer(0, status::toJson).start()) {
            app.start();

            var records = readAll(expectedIds.size(), Duration.ofMinutes(3));

            var ids = new ArrayList<String>();
            var window = new HistoryWindow(CUTOVER);
            for (var record : records) {
                ContractAssertions.assertProduces("transaction-history-record.v1", record.value());
                var message = JsonCodec.fromJson(record.value(), TransactionHistoryRecord.class);
                assertThat(record.key()).as("chave = accountId").isEqualTo(message.accountId());
                assertThat(window.contains(message.occurredAt())).as("fora da janela: %s", message).isTrue();
                assertThat(record.value()).doesNotContain("Pagador", "Borda", "payer").doesNotContainPattern(CPF);
                ids.add(message.transactionId());
            }
            assertThat(ids).as("sem repetição").doesNotHaveDuplicates();
            assertThat(new TreeSet<>(ids)).as("os mesmos ids do SQL").isEqualTo(expectedIds);

            var statusUrl = "http://localhost:" + statusServer.port() + StatusServer.PATH;
            var body = get(statusUrl);
            for (int i = 0; i < 30 && !body.contains("\"state\":\"COMPLETED\""); i++) {
                Thread.sleep(1000);
                body = get(statusUrl);
            }
            var json = JsonCodec.readTree(body);
            assertThat(json.path("state").asString()).isEqualTo("COMPLETED");
            assertThat(json.path("published").asLong()).isEqualTo(expectedIds.size());
            assertThat(json.path("skipped").path("out_of_window").asLong()).isEqualTo(ROWS - expectedIds.size());
            assertThat(json.path("cutoverAt").asString()).isEqualTo("2026-11-15T00:00:00Z");

            var metrics = get("http://localhost:" + app.httpPort() + "/actuator/prometheus");
            assertThat(metrics).contains("backfill_records_published_total " + (double) expectedIds.size());

            writeFixtureIfRequested(records);
        }
    }

    private static void writeFixtureIfRequested(List<ConsumerRecord<String, String>> records) throws Exception {
        var dir = System.getProperty("fixture.dir");
        if (dir == null || dir.isBlank()) {
            return;
        }
        Set<String> reference = new TreeSet<>();
        for (int i = 1; i <= REFERENCE_ACCOUNTS; i++) {
            reference.add("acc_tok_%08d".formatted(i));
        }
        var lines = records.stream()
                .filter(r -> reference.contains(r.key()))
                .sorted(Comparator.comparing(ConsumerRecord<String, String>::key)
                        .thenComparing(r -> JsonCodec.fromJson(r.value(), TransactionHistoryRecord.class).transactionId()))
                .map(ConsumerRecord::value).toList();
        Files.createDirectories(Path.of(dir));
        Files.write(Path.of(dir, "reference-history.jsonl"), lines);

        var expected = new LinkedHashMap<String, Object>();
        var accounts = new LinkedHashMap<String, Map<String, Long>>();
        for (var account : reference) {
            accounts.put(account, new LinkedHashMap<>(Map.of("count30d", 0L, "avgAmount30d", 0L, "maxAmount90d", 0L)));
        }
        var days30 = CUTOVER_SQL + " - 29 * 86400 * interval '1 second'";
        var days90 = CUTOVER_SQL + " - 89 * 86400 * interval '1 second'";
        for (var row : query("SELECT account_token, count(*), div(sum(amount_cents), count(*)) FROM bank.transactions WHERE "
                + "occurred_at >= " + days30 + " AND occurred_at < " + CUTOVER_SQL + " AND " + windowPredicate()
                + " GROUP BY account_token")) {
            if (accounts.containsKey(row[0])) {
                accounts.get(row[0]).put("count30d", Long.parseLong(row[1]));
                accounts.get(row[0]).put("avgAmount30d", Long.parseLong(row[2]));
            }
        }
        for (var row : query("SELECT account_token, max(amount_cents) FROM bank.transactions WHERE occurred_at >= " + days90
                + " AND occurred_at < " + CUTOVER_SQL + " AND " + windowPredicate() + " GROUP BY account_token")) {
            if (accounts.containsKey(row[0])) {
                accounts.get(row[0]).put("maxAmount90d", Long.parseLong(row[1]));
            }
        }
        expected.put("cutoverAt", CUTOVER.toString());
        expected.put("evaluatedAt", CUTOVER.toString());
        expected.put("accounts", accounts);
        Files.writeString(Path.of(dir, "reference-expected.json"), JsonCodec.mapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(expected));
    }
}
