package com.appmetr.s2s;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
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
    public static final int CONNECT_TIMEOUT = 60 * 1000;
    public static final int READ_TIMEOUT = 120 * 1000;

    private static Logger logger = LoggerFactory.getLogger(HttpRequestService.class);
    private static JsonParser jsonParser = new JsonParser();
    private final static String serverMethodName = "server.trackS2S";

    public static boolean sendRequest(String httpURL, String token, byte[] batches) throws IOException {
        Map<String, String> params = new HashMap<String, String>(2);
        params.put("method", serverMethodName);
        params.put("token", token);
        params.put("timestamp", String.valueOf(new Date().getTime()));

        URL url = new URL(httpURL + "?" + makeQueryString(params));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
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
            BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder result = new StringBuilder();
            try {
                String inputLine;
                while ((inputLine = input.readLine()) != null) {
                    result.append(inputLine);
                }
            } finally {
                input.close();
            }

            try {
                JsonObject responseJson = jsonParser.parse(result.toString()).getAsJsonObject();
                String status = null;
                if (!isError(responseJson))
                    status = responseJson.get("response").getAsJsonObject().get("status").getAsString();

                if (status != null && status.compareTo("OK") == 0) {
                    return true;
                }
            } catch (JsonSyntaxException jsonError) {
                logger.error("Json exception", jsonError);
            }
        }
        catch (SocketTimeoutException timeoutException){
            logger.error("Socket timeout", timeoutException);
        }
        catch (Exception error) {
            logger.error("Server error", error);
        } finally {
            connection.disconnect();
        }

        return false;
    }

    private static boolean isError(JsonObject response) {
        try {
            String errorMessage = response.get("error").getAsJsonObject().get("message").getAsString();
            logger.error("Cant send batch: " + errorMessage);
            return true;
        } catch (NullPointerException e) {
            return false;
        }
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
