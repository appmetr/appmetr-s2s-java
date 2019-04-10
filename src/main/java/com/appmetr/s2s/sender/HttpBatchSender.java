package com.appmetr.s2s.sender;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class HttpBatchSender implements BatchSender {
    private static final Logger log = LoggerFactory.getLogger(HttpBatchSender.class);

    protected static final ThreadLocal<byte[]> bytesThreadLocal = ThreadLocal.withInitial(() -> new byte[1024]);

    protected int connectTimeoutMs = 60 * 1000;
    protected int readTimeoutMs = 120 * 1000;
    protected ObjectMapper objectMapper = new ObjectMapper();
    protected String serverMethodName = "server.trackS2S";
    protected Clock clock = Clock.systemUTC();


    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void setServerMethodName(String serverMethodName) {
        this.serverMethodName = serverMethodName;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override public boolean send(String httpURL, String deploy, byte[] batches) {
        final HttpURLConnection connection;
        try {
            final URL url = makeUrl(httpURL, deploy);
            connection = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            log.error("Connection creation exception to {} and {}", httpURL, deploy, e);
            return false;
        }

        try {
            connection.setConnectTimeout(connectTimeoutMs);
            connection.setReadTimeout(readTimeoutMs);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/octet-stream");
            connection.setFixedLengthStreamingMode(batches.length);

            try (OutputStream out = connection.getOutputStream()) {
                out.write(batches);
            }
            
            try (InputStream inputStream = connection.getInputStream()) {
                if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    return false;
                }

                final String result = readStream(inputStream);
                final JsonNode responseJson = objectMapper.readTree(result);

                JsonNode status = null;
                if (!isError(responseJson)) {
                    status = responseJson.get("response").get("status");
                }

                if (status != null && "OK".equalsIgnoreCase(status.textValue())) {
                    return true;
                }
            } catch (JsonParseException jsonError) {
                log.error("Json exception", jsonError);
            }
        } catch (Exception e) {
            log.warn("Request exception", e);
        } finally {
            connection.disconnect();
        }

        return false;
    }

    protected boolean isError(JsonNode response) {
        final JsonNode error = response.get("error");
        if (error == null) {
            return false;
        }

        final JsonNode message = error.get("message");
        if (message != null) {
            log.error("Can't send batch: {}", message.asText());
        }

        final JsonNode stackTrace = error.get("stackTrace");
        if (stackTrace != null) {
            log.error(stackTrace.asText());
        }

        return true;
    }

    protected URL makeUrl(String httpURL, String token) throws MalformedURLException {
        final Map<String, String> params = new HashMap<>(3);
        params.put("method", serverMethodName);
        params.put("token", token);
        params.put("timestamp", String.valueOf(clock.millis()));

        return new URL(httpURL + "?" + makeQueryString(params));
    }

    protected String makeQueryString(Map<String, String> params) {
        final StringBuilder queryBuilder = new StringBuilder();
        params.forEach((k, v) -> {
            if (v != null) {
                if (queryBuilder.length() > 0) {
                    queryBuilder.append("&");
                }

                queryBuilder.append(k).append("=").append(v); //we know nothing to encode
            }
        });
        return queryBuilder.toString();
    }

    protected String readStream(InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final byte[] buffer = bytesThreadLocal.get();
            while (true) {
                final int length = inputStream.read(buffer);
                if (length == -1) {
                    break;
                }
                byteArrayOutputStream.write(buffer, 0, length);
            }
            return byteArrayOutputStream.toString(StandardCharsets.UTF_8.name());
        }
    }
}
