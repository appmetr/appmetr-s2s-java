package com.appmetr.s2s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HttpRequestService {
    private static final Logger log = LoggerFactory.getLogger(HttpRequestService.class);

    public static final int CONNECT_TIMEOUT = 60 * 1000;
    public static final int READ_TIMEOUT = 120 * 1000;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final static String serverMethodName = "server.trackS2S";

    public static boolean sendRequest(String httpURL, String token, byte[] batches) throws IOException {
        final Map<String, String> params = new HashMap<>(2);
        params.put("method", serverMethodName);
        params.put("token", token);
        params.put("timestamp", String.valueOf(new Date().getTime()));

        final URL url = new URL(httpURL + "?" + makeQueryString(params));
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(CONNECT_TIMEOUT);
        connection.setReadTimeout(READ_TIMEOUT);

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
            final StringBuilder result = new StringBuilder();
            try (BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                String inputLine;
                while ((inputLine = input.readLine()) != null) {
                    result.append(inputLine);
                }
            }

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
        }
        catch (SocketTimeoutException timeoutException){
            log.error("Socket timeout", timeoutException);
        }
        catch (Exception error) {
            log.error("Server error", error);
        } finally {
            connection.disconnect();
        }

        return false;
    }

    private static boolean isError(JsonNode response) {
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

    private static String makeQueryString(Map<String, String> params) {
        StringBuilder queryBuilder = new StringBuilder();
        int paramCount = 0;
        for (Map.Entry<String, String> param : params.entrySet()) {
            if (param.getValue() != null) {
                paramCount++;
                if (paramCount > 1) {
                    queryBuilder.append("&");
                }
                try {
                    queryBuilder.append(param.getKey()).append("=").append(URLEncoder.encode(param.getValue(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return queryBuilder.toString();
    }
}
