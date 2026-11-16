package br.com.motorfraude.transaction.adapters.controller.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StatusServerTest {

    private final AtomicReference<String> body = new AtomicReference<>("{\"state\":\"RUNNING\"}");
    private StatusServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() {
        server = new StatusServer(0, body::get).start();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    private HttpResponse<String> send(String method, String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("GET /v1/backfill/status devolve o JSON atual, sempre fresco")
    void status() throws Exception {
        var first = send("GET", "/v1/backfill/status");
        assertThat(first.statusCode()).isEqualTo(200);
        assertThat(first.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).startsWith("application/json"));
        assertThat(first.body()).isEqualTo("{\"state\":\"RUNNING\"}");

        body.set("{\"state\":\"COMPLETED\"}");

        assertThat(send("GET", "/v1/backfill/status").body()).isEqualTo("{\"state\":\"COMPLETED\"}");
    }

    @Test
    @DisplayName("outro caminho dá 404 e outro método dá 405")
    void outros() throws Exception {
        assertThat(send("GET", "/v1/outra-coisa").statusCode()).isEqualTo(404);
        var post = send("POST", "/v1/backfill/status");
        assertThat(post.statusCode()).isEqualTo(405);
        assertThat(post.headers().firstValue("Allow")).hasValue("GET");
    }
}
