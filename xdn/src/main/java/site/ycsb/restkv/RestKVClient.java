package site.ycsb.restkv;

import site.ycsb.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class RestKVClient extends DB {
  int TIMEOUT_DURATION = 3000;

  public RestKVClient() {
    System.out.println(">>> Using XDN RestKVClient!");
  }

  private String endpoint;

  @Override
  public void init() throws DBException {
    endpoint = getProperties().getProperty("xdn.restkv.endpoint", "http://restkv.xdnapp.com:2301");
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    HttpURLConnection conn = null;

    try {
      URI uri = URI.create(String.format("%s/api/kv/%s", endpoint, key));
      System.out.printf("Read %s\n", uri.toURL());
      conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod("GET");
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

  private String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      URL url = URI.create(String.format("%s/api/kv/%s", endpoint, key)).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setConnectTimeout(TIMEOUT_DURATION);
      conn.setReadTimeout(TIMEOUT_DURATION);

      String value = values.toString();
      String jsonBody = String.format("{\"key\": \"%s\", \"value\": \"%s\"}", key, escapeJson(value));

      System.out.printf("Insert %s: %s\n", url, value);
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
  public Status delete(String table, String key) {
    try {
      URL url = URI.create(String.format("%s/api/kv/%s", endpoint, key)).toURL();
      System.out.printf("Delete %s\n", url);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("DELETE");
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
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setConnectTimeout(TIMEOUT_DURATION);
      conn.setReadTimeout(TIMEOUT_DURATION);

      String value = values.toString();
      String jsonBody = String.format("{\"key\": \"%s\", \"value\": \"%s\"}", key, escapeJson(value));

      System.out.printf("Update %s: %s\n", url, value);
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
    System.out.println("WARN: scan() is not implemented");
    return Status.OK;
  }
}
