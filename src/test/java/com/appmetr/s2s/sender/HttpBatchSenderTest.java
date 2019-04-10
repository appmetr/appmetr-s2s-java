package com.appmetr.s2s.sender;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

class HttpBatchSenderTest {

    @Test
    void send() {
        WireMockServer wireMockServer = new WireMockServer(0);
        wireMockServer.start();

        configureFor(wireMockServer.port());
        
        stubFor(get("/api").willReturn(aResponse().withStatus(200)));

        verify(1, getRequestedFor(urlEqualTo("/api")));

        wireMockServer.stop();
    }
}
