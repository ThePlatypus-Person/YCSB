package site.ycsb.restkv;

import com.fasterxml.jackson.databind.ObjectMapper;
import site.ycsb.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class RestKVClient extends DB {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  int TIMEOUT_DURATION = 3000;

  public RestKVClient() {
    System.out.println(">>> Using XDN RestKVClient!");
  }

  private String endpoint;

  @Override
  public void init() throws DBException {
    endpoint = getProperties().getProperty("xdn.restkv.endpoint", "http://restkv.xdnapp.com:2300");
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    HttpURLConnection conn = null;

    try {
      URI uri = URI.create(String.format("%s/api/kv/%s", endpoint, key));
      //System.out.printf("Read %s\n", uri.toURL());
      conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("XDN", "restkv");
      conn.setConnectTimeout(TIMEOUT_DURATION);
      conn.setReadTimeout(TIMEOUT_DURATION);

      int responseCode = conn.getResponseCode();
      if (responseCode == 200) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
          StringBuilder response = new StringBuilder();
          String line;
          while ((line = in.readLine()) != null) {
            response.append(line);
          }
          result.put("data", new StringByteIterator(response.toString()));
        }
        return Status.OK;
      } else {
        // Optional: log error response body for debugging
        try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
          StringBuilder errorResponse = new StringBuilder();
          String line;
          while ((line = errorReader.readLine()) != null) {
            errorResponse.append(line);
          }
          System.err.println("HTTP error response: " + errorResponse);
        }
        return Status.ERROR;
      }

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String output = "";
    try {
      URL url = URI.create(String.format("%s/api/kv/%s", endpoint, key)).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("XDN", "restkv");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      conn.setConnectTimeout(TIMEOUT_DURATION);
      conn.setReadTimeout(TIMEOUT_DURATION);

      String rawValue = values.toString();
      String encoded = Base64.getEncoder().encodeToString(rawValue.getBytes(StandardCharsets.UTF_8));
      Map<String, String> body = new HashMap<>();
      body.put("key", key);
      body.put("value", encoded);
      String jsonBody = objectMapper.writeValueAsString(body);
      output = jsonBody;

      //System.out.printf("Insert %s: %s\n", url, jsonBody);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }

      int responseCode = conn.getResponseCode();
      return (responseCode == 200 || responseCode == 201) ? Status.OK : Status.ERROR;

    } catch (Exception e) {
      String url = String.format("%s/api/kv/%s", endpoint, key);
      System.out.printf("FAILED Insert %s: %s\n", url, output);

      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      URL url = URI.create(String.format("%s/api/kv/%s", endpoint, key)).toURL();
      //System.out.printf("Delete %s\n", url);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("DELETE");
      conn.setRequestProperty("XDN", "restkv");
      conn.setConnectTimeout(TIMEOUT_DURATION);
      conn.setReadTimeout(TIMEOUT_DURATION);

      int responseCode = conn.getResponseCode();
      return (responseCode == 200 || responseCode == 201) ? Status.OK : Status.ERROR;

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      URL url = URI.create(String.format("%s/api/kv/%s", endpoint, key)).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("XDN", "restkv");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setConnectTimeout(TIMEOUT_DURATION);
      conn.setReadTimeout(TIMEOUT_DURATION);

      String rawValue = values.toString();
      String encoded = Base64.getEncoder().encodeToString(rawValue.getBytes(StandardCharsets.UTF_8));
      Map<String, String> body = new HashMap<>();
      body.put("key", key);
      body.put("value", encoded);
      String jsonBody = objectMapper.writeValueAsString(body);

      //System.out.printf("Update %s: %s\n", url, jsonBody);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }

      int responseCode = conn.getResponseCode();
      return (responseCode == 200 || responseCode == 201) ? Status.OK : Status.ERROR;


    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startKey, int recordCount,
    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
