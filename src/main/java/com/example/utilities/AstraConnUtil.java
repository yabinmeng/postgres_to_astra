package com.example.utilities;

import com.datastax.oss.driver.api.core.CqlSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;

public class AstraConnUtil {
    private final static boolean debug = true;

    // Astra database keyspace, username and password
    final static String astraKeyspace = "testks";
    final static String astraDBUserName = "demo";
    final static String astraDBUserPwd = "demo123";

    // Astra connection settings for secure connection bundle
    final static String secureConnBundleFile = "/Users/yabinmeng/Downloads/secure-connect-ymhtdemo2.zip";

    // Astra Stargate API connection settings
    final static String astraDbId = "2d9bdb01-216e-48a6-aa19-bc7963e21c47";
    final static String astraRegion = "us-east1";

    final static String astraStargateAPIBase = "https://" +
            astraDbId + "-" + astraRegion + "." +
            "apps.astra.datastax.com";

    // Get a CqlSession using the secure connection bundle
    public static CqlSession getCqlSession() {
        CqlSession session = null;

        session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(secureConnBundleFile))
                .withAuthCredentials(astraDBUserName,astraDBUserPwd)
                .withKeyspace(astraKeyspace)
                .build();

        if (session != null) {
            System.out.println("   --> Connection established.");
        }
        else {
            System.out.println("   --> Connection failed.");
        }

        return session;
    }

    // Close a CqlSession
    public static void closeCqlSession(CqlSession cqlSession) {
        if (cqlSession != null) {
            cqlSession.close();
        }
    }


    // ===========================================
    // Stargate Document API methods
    // ===========================================
    public static JSONObject makeAPICall(String method, String token, String apiStr, JSONObject payload) {
        JSONObject rtnJsonObj = null;

        try {
            URL url = new URL(astraStargateAPIBase + apiStr);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod(method);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "*/*");
            connection.setRequestProperty("Connection", "keep-alive");
            // For APIs that require authToken, add it
            if (! apiStr.contains("auth")) {
                assert (token != null);
                connection.setRequestProperty("X-Cassandra-Token", token);
            }

            // debug HTTP rest API call
            if (debug) {
                System.out.println("   --> Making API call: " + astraStargateAPIBase + apiStr);
                System.out.println("               Payload: " + payload);
                System.out.println("                 token: " + token);
                System.out.println("       HTTP.request.method = " + connection.getRequestMethod());
                System.out.println("       HTTP.request.properties = " + connection.getRequestProperties());
            }

            if (payload != null) {
                OutputStream os = connection.getOutputStream();
                os.write(payload.toJSONString().getBytes());
                os.flush();
            }

            int responseCode = connection.getResponseCode();
            if (connection.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                System.out.println("       HTTP.response.code = " + responseCode);

                BufferedReader br = new BufferedReader(
                        new InputStreamReader((connection.getErrorStream())));

                StringBuilder errorStr = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    errorStr.append(responseLine.trim());
                }
                System.out.println("       HTTP.response.error_message = " + errorStr.toString());
            }
            else {
                BufferedReader br = new BufferedReader(
                        new InputStreamReader((connection.getInputStream())));

                StringBuilder responseStr = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    responseStr.append(responseLine.trim());
                }

                JSONParser parser = new JSONParser();
                try {
                    rtnJsonObj = (JSONObject) parser.parse(responseStr.toString());
                } catch (ParseException e) {
                    e.printStackTrace();
                    rtnJsonObj = null;
                }

            }

            connection.disconnect();
        }
        catch (IOException ioException) {
            ioException.printStackTrace();
        }

        return rtnJsonObj;
    }

    // Get Access Token
    public static String getAccessToken() {
        String authTokenStr = null;

        String authAPIStr = "/api/rest/v1/auth";

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("username", astraDBUserName);
        jsonObj.put("password", astraDBUserPwd);

        JSONObject response = makeAPICall("POST", null, authAPIStr, jsonObj);
        if (response != null) {
            authTokenStr = (String)response.get("authToken");
        }

        return authTokenStr;
    }

    // Write a "document" (row) into a table
    public static void WriteDocument(String accessToken, String tableName, JSONObject actor) {
        int actor_id = (Integer)actor.get("actor_id");

        String authAPIStr =
                "/api/rest/v2/namespaces/" + astraKeyspace +
                "/collections/" + tableName + "/" +
                actor_id;
        actor.remove("actor_id");

        JSONObject response = makeAPICall("PUT", accessToken, authAPIStr, actor);
    }
}
