package com.appmetr.s2s.sender;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

class HttpBatchSenderIT {

    static String path = "/api";
    static String token = "testToken";
    static String method = "server.trackS2S";
    static byte[] batch = new byte[]{1};

    static WireMockServer wireMockServer;
    static String url;

    static String JSON_OK = "{\"response\": {\"status\": \"OK\"}}";

    HttpBatchSender httpBatchSender = new HttpBatchSender();

    @BeforeAll
    static void setUpAll() {
        wireMockServer = new WireMockServer(0);
        wireMockServer.start();
        url = wireMockServer.baseUrl() + path;

        configureFor(wireMockServer.port());
    }

    @AfterAll
    static void tearDownAll() {
        shutdownServer();
    }

    @BeforeEach
    void setUp() {
        reset();
    }

    @Test
    void send() {
        stubFor(post(urlPathEqualTo(path)).willReturn(okJson(JSON_OK)));

        httpBatchSender.setClock(Clock.fixed(Instant.ofEpochSecond(1), ZoneOffset.UTC));

        Assertions.assertTrue(httpBatchSender.send(url, token, batch));

        verify(postRequestedFor(urlPathEqualTo(path))
                .withQueryParam("method", equalTo(method))
                .withQueryParam("token", equalTo(token))
                .withQueryParam("timestamp", equalTo("1000")));
    }
}
