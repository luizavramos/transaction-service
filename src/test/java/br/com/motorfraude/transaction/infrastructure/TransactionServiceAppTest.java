package br.com.motorfraude.transaction.infrastructure;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransactionServiceAppTest {

    @Test
    @DisplayName("sem CUTOVER_AT o serviço recusa a partida antes de abrir porta ou falar com o Kafka")
    void semCorte() {
        assertThatThrownBy(() -> TransactionServiceApp.run(Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka-inexistente:9092")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CUTOVER_AT");
    }

    @Test
    @DisplayName("CUTOVER_AT inválido também recusa a partida")
    void corteInvalido() {
        assertThatThrownBy(() -> TransactionServiceApp.run(Map.of("CUTOVER_AT", "amanhã")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CUTOVER_AT");
    }
}
