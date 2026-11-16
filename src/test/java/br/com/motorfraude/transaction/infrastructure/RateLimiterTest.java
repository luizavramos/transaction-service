package br.com.motorfraude.transaction.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RateLimiterTest {

    /** Relógio falso: dormir apenas avança o tempo, sem esperar de verdade. */
    private static final class FakeTime {
        long nanos;

        long now() {
            return nanos;
        }

        void sleep(long sleepNanos) {
            nanos += sleepNanos;
        }
    }

    @Test
    @DisplayName("dentro do balde (burst) não espera")
    void semEsperaNoBurst() {
        var time = new FakeTime();
        var limiter = new RateLimiter(1000, 100, time::now, time::sleep);

        for (int i = 0; i < 100; i++) {
            limiter.acquire();
        }

        assertThat(time.nanos).isZero();
    }

    @Test
    @DisplayName("20 mil por segundo: 40 mil chamadas levam ~1,9 s (as 2 mil primeiras saem do balde)")
    void respeitaATaxa() {
        var time = new FakeTime();
        var limiter = new RateLimiter(20_000, 2_000, time::now, time::sleep);

        for (int i = 0; i < 40_000; i++) {
            limiter.acquire();
        }

        assertThat(time.nanos).isBetween(TimeUnit.MILLISECONDS.toNanos(1_890), TimeUnit.MILLISECONDS.toNanos(1_950));
    }

    @Test
    @DisplayName("depois de ficar parado o balde enche de novo, mas nunca passa do burst")
    void balde() {
        var time = new FakeTime();
        var limiter = new RateLimiter(1000, 10, time::now, time::sleep);
        for (int i = 0; i < 10; i++) {
            limiter.acquire();
        }

        time.nanos += TimeUnit.SECONDS.toNanos(60);
        var before = time.nanos;
        for (int i = 0; i < 10; i++) {
            limiter.acquire();
        }
        assertThat(time.nanos).isEqualTo(before);

        limiter.acquire();
        assertThat(time.nanos).isGreaterThan(before);
    }

    @Test
    @DisplayName("taxa ou burst menor que 1 é rejeitado")
    void invalido() {
        assertThatThrownBy(() -> new RateLimiter(0, 10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RateLimiter(10, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
