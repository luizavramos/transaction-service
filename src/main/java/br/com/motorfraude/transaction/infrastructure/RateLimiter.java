package br.com.motorfraude.transaction.infrastructure;

import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Balde de fichas: repõe {@code permitsPerSecond} fichas por segundo, até {@code burst}. {@link #acquire()} bloqueia a
 * thread quando o balde esvazia, o que na topologia faz o consumo do tópico de origem andar na velocidade do limite
 * (o motor não é inundado de histórico enquanto atende o tráfego ao vivo).
 */
public final class RateLimiter {

    private final double permitsPerNano;
    private final double burst;
    private final LongSupplier nanoClock;
    private final LongConsumer sleeper;
    private double tokens;
    private long last;

    public RateLimiter(int permitsPerSecond, int burst) {
        this(permitsPerSecond, burst, System::nanoTime, LockSupport::parkNanos);
    }

    RateLimiter(int permitsPerSecond, int burst, LongSupplier nanoClock, LongConsumer sleeper) {
        if (permitsPerSecond < 1) {
            throw new IllegalArgumentException("permitsPerSecond deve ser 1 ou mais");
        }
        if (burst < 1) {
            throw new IllegalArgumentException("burst deve ser 1 ou mais");
        }
        this.permitsPerNano = permitsPerSecond / 1_000_000_000.0;
        this.burst = burst;
        this.nanoClock = nanoClock;
        this.sleeper = sleeper;
        this.tokens = burst;
        this.last = nanoClock.getAsLong();
    }

    public synchronized void acquire() {
        refill();
        tokens -= 1;
        if (tokens < 0) {
            var waitNanos = (long) Math.ceil(-tokens / permitsPerNano);
            sleeper.accept(waitNanos);
            refill();
        }
    }

    private void refill() {
        var now = nanoClock.getAsLong();
        tokens = Math.min(burst, tokens + (now - last) * permitsPerNano);
        last = now;
    }
}
