package br.com.motorfraude.transaction.infrastructure;

import br.com.motorfraude.transaction.adapters.controller.cdc.CdcFormatException;
import br.com.motorfraude.transaction.adapters.controller.cdc.DebeziumTransactionMapper;
import br.com.motorfraude.transaction.adapters.gateway.kafka.HistoryRecordSerde;
import br.com.motorfraude.transaction.application.gateway.BackfillMetrics;
import br.com.motorfraude.transaction.application.usecase.BackfillTransactionHistoryUseCase;
import br.com.motorfraude.transaction.domain.HistoryRecord;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topologia sem estado: lê o CDC das transações, deixa passar só o snapshot dentro da janela e publica a chave
 * {@code accountId}, a mesma do motor (o tópico de destino é co-particionado com o dele).
 */
public final class TopologyFactory {

    public static final String SOURCE_TOPIC = "cdc.bank.transactions";
    public static final String BACKFILL_TOPIC = "transaction-history-backfill.v1";

    private static final Logger LOG = LoggerFactory.getLogger(TopologyFactory.class);

    private TopologyFactory() {
    }

    public static Topology create(HistoryWindow window, BackfillMetrics metrics, BackfillStatus status, RateLimiter limiter) {
        var useCase = new BackfillTransactionHistoryUseCase(window);
        var builder = new StreamsBuilder();

        builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null)
                .flatMap((key, value) -> {
                    try {
                        var change = DebeziumTransactionMapper.parse(value).orElseThrow();
                        var outcome = useCase.evaluate(change);
                        List<KeyValue<String, HistoryRecord>> out = List.of();
                        if (outcome.published()) {
                            limiter.acquire();
                            out = List.of(KeyValue.pair(outcome.record().accountId(), outcome.record()));
                            metrics.published();
                            status.published();
                        } else {
                            metrics.skipped(outcome.skipReason());
                            status.skipped(outcome.skipReason());
                        }
                        // o fim do snapshot conta mesmo quando o último registro cai fora da janela
                        if (change.lastOfSnapshot()) {
                            status.snapshotCompleted();
                        }
                        return out;
                    } catch (CdcFormatException e) {
                        LOG.warn("evento do CDC descartado: {}", e.getMessage());
                        metrics.malformed();
                        status.malformed();
                        return List.<KeyValue<String, HistoryRecord>>of();
                    }
                })
                .to(BACKFILL_TOPIC, Produced.with(Serdes.String(), new HistoryRecordSerde()));

        return builder.build();
    }
}
