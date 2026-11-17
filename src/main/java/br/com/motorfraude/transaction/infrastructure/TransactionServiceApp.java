package br.com.motorfraude.transaction.infrastructure;

import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;
import br.com.motorfraude.transaction.adapters.controller.rest.StatusServer;
import br.com.motorfraude.transaction.adapters.gateway.micrometer.MicrometerBackfillMetrics;
import br.com.motorfraude.transaction.domain.HistoryWindow;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;

public final class TransactionServiceApp {

    public static final String APPLICATION_ID = "transaction-service";
    static final String STATUS_FILE = "backfill-status.json";

    private TransactionServiceApp() {
    }

    public static void main(String[] args) {
        run(System.getenv());
    }

    static void run(Map<String, String> env) {
        var backfill = BackfillSettings.fromEnvironment(env);
        var settings = StreamsSettings.fromEnvironment(APPLICATION_ID, env);
        var status = new BackfillStatus(backfill.cutoverAt(), Clock.systemUTC(), Path.of(settings.stateDir().toString(), STATUS_FILE),
                backfill.completionQuiet());
        var limiter = new RateLimiter(backfill.ratePerSecond(), Math.max(1, backfill.ratePerSecond() / 10));

        var app = StreamsAppBootstrap.create(settings, registry -> TopologyFactory.create(
                new HistoryWindow(backfill.cutoverAt()), new MicrometerBackfillMetrics(registry), status, limiter));
        app.start();
        var statusServer = new StatusServer(backfill.statusPort(), status::toJson).start();
        Runtime.getRuntime().addShutdownHook(new Thread(statusServer::close, "status-shutdown"));
    }
}
