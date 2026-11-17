package br.com.motorfraude.transaction.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.transaction.application.usecase.SkipReason;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BackfillStatusTest {

    private static final Instant CUTOVER = Instant.parse("2026-11-15T00:00:00Z");
    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path dir;

    @Test
    @DisplayName("começa RUNNING, com tudo zerado, o cutoverAt e o início da janela")
    void inicio() {
        var status = new BackfillStatus(CUTOVER, clock, null);

        var snapshot = status.snapshot();
        assertThat(snapshot.state()).isEqualTo("RUNNING");
        assertThat(snapshot.cutoverAt()).isEqualTo(CUTOVER);
        assertThat(snapshot.windowStart()).isEqualTo(Instant.parse("2026-08-17T00:00:00Z"));
        assertThat(snapshot.published()).isZero();
        assertThat(snapshot.skipped()).containsEntry("not_snapshot", 0L).containsEntry("out_of_window", 0L)
                .containsEntry("invalid_record", 0L);
        assertThat(snapshot.malformed()).isZero();
        assertThat(snapshot.completedAt()).isNull();
    }

    @Test
    @DisplayName("conta publicados, descartados por motivo e eventos malformados")
    void contadores() {
        var status = new BackfillStatus(CUTOVER, clock, null);

        status.published();
        status.published();
        status.skipped(SkipReason.OUT_OF_WINDOW);
        status.skipped(SkipReason.NOT_SNAPSHOT);
        status.skipped(SkipReason.OUT_OF_WINDOW);
        status.malformed();

        var snapshot = status.snapshot();
        assertThat(snapshot.published()).isEqualTo(2);
        assertThat(snapshot.skipped()).containsEntry("out_of_window", 2L).containsEntry("not_snapshot", 1L);
        assertThat(snapshot.malformed()).isEqualTo(1);
    }

    @Test
    @DisplayName("snapshotCompleted vira COMPLETED com o instante, e é idempotente")
    void completa() {
        var status = new BackfillStatus(CUTOVER, clock, null);
        status.published();

        status.snapshotCompleted();
        status.snapshotCompleted();

        var snapshot = status.snapshot();
        assertThat(snapshot.state()).isEqualTo("COMPLETED");
        assertThat(snapshot.completedAt()).isEqualTo(NOW);
        assertThat(snapshot.published()).isEqualTo(1);
    }

    @Test
    @DisplayName("o JSON traz estado, totais, cutoverAt e início da janela")
    void json() {
        var status = new BackfillStatus(CUTOVER, clock, null);
        status.published();
        status.snapshotCompleted();

        var json = status.toJson();

        assertThat(json).contains("\"state\":\"COMPLETED\"").contains("\"published\":1")
                .contains("\"cutoverAt\":\"2026-11-15T00:00:00Z\"").contains("\"windowStart\":\"2026-08-17T00:00:00Z\"")
                .contains("\"completedAt\":\"2026-11-16T10:00:00Z\"");
    }

    @Test
    @DisplayName("depois de reiniciar, COMPLETED e os totais voltam do arquivo (o último evento do snapshot não vem de novo)")
    void sobreviveAoReinicio() {
        var file = dir.resolve("backfill-status.json");
        var first = new BackfillStatus(CUTOVER, clock, file);
        first.published();
        first.published();
        first.skipped(SkipReason.OUT_OF_WINDOW);
        first.snapshotCompleted();
        first.snapshot();

        var second = new BackfillStatus(CUTOVER, clock, file);

        var snapshot = second.snapshot();
        assertThat(snapshot.state()).isEqualTo("COMPLETED");
        assertThat(snapshot.published()).isEqualTo(2);
        assertThat(snapshot.skipped()).containsEntry("out_of_window", 1L);
        assertThat(snapshot.completedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("arquivo de outro cutoverAt é ignorado: o que estava guardado era de outra janela")
    void outroCorte() {
        var file = dir.resolve("backfill-status.json");
        var old = new BackfillStatus(Instant.parse("2026-10-01T00:00:00Z"), clock, file);
        old.published();
        old.snapshotCompleted();
        old.snapshot();

        var fresh = new BackfillStatus(CUTOVER, clock, file);

        assertThat(fresh.snapshot().state()).isEqualTo("RUNNING");
        assertThat(fresh.snapshot().published()).isZero();
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(java.time.Duration by) {
            now = now.plus(by);
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @Test
    @DisplayName("o tópico tem 3 partições: com janela de silêncio, o último evento sozinho não completa; só depois de ela passar sem evento novo")
    void janelaDeSilencio() {
        var time = new MutableClock(NOW);
        var status = new BackfillStatus(CUTOVER, time, null, java.time.Duration.ofSeconds(10));
        status.published();
        status.snapshotCompleted();

        assertThat(status.snapshot().state()).isEqualTo("RUNNING");

        time.advance(java.time.Duration.ofSeconds(9));
        assertThat(status.snapshot().state()).isEqualTo("RUNNING");

        time.advance(java.time.Duration.ofSeconds(1));
        var snapshot = status.snapshot();
        assertThat(snapshot.state()).isEqualTo("COMPLETED");
        assertThat(snapshot.completedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("registro que chega depois do último evento (outra partição atrasada) volta o estado para RUNNING")
    void retardatario() {
        var time = new MutableClock(NOW);
        var status = new BackfillStatus(CUTOVER, time, null, java.time.Duration.ofSeconds(10));
        status.snapshotCompleted();
        time.advance(java.time.Duration.ofSeconds(11));
        assertThat(status.snapshot().state()).isEqualTo("COMPLETED");

        status.published();

        assertThat(status.snapshot().state()).isEqualTo("RUNNING");
        time.advance(java.time.Duration.ofSeconds(10));
        assertThat(status.snapshot().state()).isEqualTo("COMPLETED");
        assertThat(status.snapshot().published()).isEqualTo(1);
    }

    @Test
    @DisplayName("COMPLETED só é gravado em disco quando de fato completa, não ao ver o último evento")
    void gravaSoAoCompletar() throws Exception {
        var file = dir.resolve("backfill-status.json");
        var time = new MutableClock(NOW);
        var status = new BackfillStatus(CUTOVER, time, file, java.time.Duration.ofSeconds(10));
        status.snapshotCompleted();
        status.snapshot();
        assertThat(file).doesNotExist();

        time.advance(java.time.Duration.ofSeconds(10));
        status.snapshot();

        assertThat(file).exists();
        assertThat(new BackfillStatus(CUTOVER, time, file, java.time.Duration.ofSeconds(10)).snapshot().state())
                .isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("arquivo corrompido não impede a partida")
    void arquivoCorrompido() throws Exception {
        var file = dir.resolve("backfill-status.json");
        Files.writeString(file, "{não é json");

        var status = new BackfillStatus(CUTOVER, clock, file);

        assertThat(status.snapshot().state()).isEqualTo("RUNNING");
    }
}
