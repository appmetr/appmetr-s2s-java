package com.appmetr;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.logging.Logger;

public class HtmlPusherService {
    private static Logger logger = Logger.getLogger("HtmlPusherService");

    public static void sendGzippedPost(String callbackUrl, String message) {
        HttpPost request = new HttpPost(callbackUrl);
        request.setEntity(new ByteArrayEntity(SerializationUtils.serializeGzip(message)));

        String responseBody = null;
        try {
            HttpParams httpParams = new BasicHttpParams();
            HttpClient httpClient = new DefaultHttpClient(httpParams);
            responseBody = httpClient.execute(request, new BasicResponseHandler());
            logger.info("Push block result callback returned [" + responseBody + "]");
        } catch (Exception e) {
            logger.info("Unexpected error while execute ApiRequest (push message to server). " + e);
            throw new RuntimeException("Unexpected exception in pushResults.", e);
        }
    }

    public boolean sendRequest(List<NameValuePair> parameters, byte[] batches) throws IOException {
        URL url = new URL(getUrlPath(parameters));
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
                String status = new JSONObject(result.toString()).getJSONObject("response").getString("status");
                if (status != null && status.compareTo("OK") == 0) {
                    return true;
                }
            } catch (JSONException jsonError) {
                // nothing to do
            }

            if (BuildConfig.DEBUG) {
                Log.d(TAG, "Invalid server response: " + result);
            }
        } catch (Exception error) {
            Log.e(TAG, "Server error", error);
            if (BuildConfig.DEBUG) {
                Log.d(TAG,
                        "Please, check rights for the app in AndroidManifest.xml."
                                + " For the app to have access to the network the uses permission \"android.permission.INTERNET\" "
                                + "must be set. You can find a detailed description here: http://developer.android.com/reference/android/Manifest.permission.html#INTERNET");
            }
        } finally {
            connection.disconnect();
        }

        return false;
    }

    protected String getUrlPath(List<NameValuePair> parameters) {
        String res = "";
        for (NameValuePair pair : parameters) {
            if (res.length() > 0) {
                res += "&";
            }

            String value = pair.getValue();
            if (value != null) {

                res += URLEncoder.encode(pair.getName()) + "=" + URLEncoder.encode(value);
            } else {
                Log.e(TAG, "Invalid parameter " + pair.getName());
            }
        }

        return mUrlPath + "?" + res;
    }
}
