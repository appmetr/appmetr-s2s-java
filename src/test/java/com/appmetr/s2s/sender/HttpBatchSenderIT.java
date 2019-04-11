package com.appmetr.s2s.sender;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

class HttpBatchSenderIT {

    static String path = "/api";
    static String token = "testToken";
    static byte[] batch = new byte[]{1};

    static WireMockServer wireMockServer;
    static String url;

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
        wireMockServer.stop();
    }

    @BeforeEach
    void setUp() {
        reset();
    }

    @Test
    void send() {
        stubFor(post(urlPathEqualTo(path)).willReturn(aResponse().withStatus(200)));

        httpBatchSender.send(url, token, batch);

        verify(postRequestedFor(urlPathEqualTo(path)));
    }
}
