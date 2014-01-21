package com.appmetr;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.oracle.javafx.jmx.json.JSONException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class HttpRequestService {
    private static Logger logger = Logger.getLogger("HttpRequestService");
    private final static String serverMethodName = "server.trackS2S";

    public static boolean sendRequest(String callbackUrl, String token, byte[] batches) throws IOException {
        Map<String, String> params = new HashMap<String, String>(2);
        params.put("method", serverMethodName);
        params.put("token", token);

        URL url = new URL(callbackUrl + "?" + makeQueryString(params));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

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
                JsonParser jsonParser = new JsonParser();
                JsonObject responseJson = jsonParser.parse(result.toString()).getAsJsonObject();
                String status = responseJson.get("response").getAsJsonObject().get("status").getAsString();
                if (status != null && status.compareTo("OK") == 0) {
                    return true;
                }
            } catch (JSONException jsonError) {
                // nothing to do
            }

//            if (BuildConfig.DEBUG) {
//                Log.d(TAG, "Invalid server response: " + result);
//            }
        } catch (Exception error) {
            logger.info("EXCEPTION " + error);
//            Log.e(TAG, "Server error", error);
//            if (BuildConfig.DEBUG) {
//                Log.d(TAG,
//                        "Please, check rights for the app in AndroidManifest.xml."
//                                + " For the app to have access to the network the uses permission \"android.permission.INTERNET\" "
//                                + "must be set. You can find a detailed description here: http://developer.android.com/reference/android/Manifest.permission.html#INTERNET");
//            }
        } finally {
            connection.disconnect();
        }

        return false;
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
