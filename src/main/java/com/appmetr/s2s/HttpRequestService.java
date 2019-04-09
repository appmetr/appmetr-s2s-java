package com.appmetr.s2s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class HttpRequestService {
    private static final Logger log = LoggerFactory.getLogger(HttpRequestService.class);

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

    public boolean sendRequest(String httpURL, String token, byte[] batches) throws IOException {
        final URL url = makeUrl(httpURL, token);
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(connectTimeoutMs);
        connection.setReadTimeout(readTimeoutMs);

        try {
            // Add body data
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/octet-stream");

            connection.setFixedLengthStreamingMode(batches.length);
            OutputStream out = connection.getOutputStream();
            out.write(batches);
            out.close();

            // Execute HTTP Post Request
            final String result = readResponse(connection.getInputStream());

            try {
                final JsonNode responseJson = objectMapper.readTree(result.toString());
                JsonNode status = null;
                if (!isError(responseJson)) {
                    status = responseJson.get("response").get("status");
                }

                if (status != null && "OK".equalsIgnoreCase(status.textValue())) {
                    return true;
                }
            } catch (Exception jsonError) {
                log.error("Json exception", jsonError);
            }
        } catch (Exception error) {
            log.error("Server error", error);
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

    protected String readResponse(InputStream inputStream) throws IOException {
        final StringBuilder result = new StringBuilder();
        try (BufferedReader input = new BufferedReader(new InputStreamReader(inputStream))) {
            String inputLine;
            while ((inputLine = input.readLine()) != null) {
                result.append(inputLine);
            }
        }
        return result.toString();
    }
}
