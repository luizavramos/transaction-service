package br.com.motorfraude.transaction.adapters.controller.rest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Serve só {@code GET /v1/backfill/status}. Fica numa porta própria porque o servidor de saúde e métricas do
 * {@code platform-streams} é compartilhado e não tem ponto de extensão para rotas do serviço.
 */
public final class StatusServer implements AutoCloseable {

    public static final String PATH = "/v1/backfill/status";

    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Supplier<String> statusJson;

    public StatusServer(int port, Supplier<String> statusJson) {
        this.statusJson = statusJson;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new IllegalStateException("não foi possível abrir a porta " + port, e);
        }
        server.setExecutor(executor);
        server.createContext("/", this::handle);
    }

    public StatusServer start() {
        server.start();
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                reply(exchange, 405, "text/plain; charset=utf-8", "método não permitido");
            } else if (PATH.equals(exchange.getRequestURI().getPath())) {
                reply(exchange, 200, "application/json", statusJson.get());
            } else {
                reply(exchange, 404, "text/plain; charset=utf-8", "não encontrado");
            }
        }
    }

    private static void reply(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        var bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
