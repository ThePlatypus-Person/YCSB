package site.ycsb.paxi;

import site.ycsb.*;
import java.io.*;
import java.net.*;
import java.util.*;

public class PaxiClient extends DB {
  public PaxiClient() {
    System.out.println(">>> Using PaxiClient!");
  }

  private String endpoint;

  @Override
  public void init() throws DBException {
    endpoint = getProperties().getProperty("rest.endpoint", "http://localhost:8080");
  }

  /*
  private String buildKey(String table, String key) {
    return endpoint + "/" + table + "/" + key;
  }
  */

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String numericKey = key.replace("user", "");

    HttpURLConnection conn = null;

    try {
      //URI uri = URI.create(buildKey(table, numericKey));
      URI uri = URI.create(endpoint + "/" + numericKey);
      //System.out.printf("Read %s\n", uri.toURL());
      conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod("GET");

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
    String numericKey = key.replace("user", "");

    try {
      URL url = URI.create(endpoint + "/" + numericKey).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");

      /*
      ByteIterator byteIterator = values.get("data");
      if (byteIterator == null) {
        System.out.printf("Insert error: byteIterator is null. %s\n", values.toString());
        return Status.ERROR;
      }

      String value = byteIterator.toString();
      */
      String value = values.toString();
      //System.out.printf("Insert %s: %s\n", url, value);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(value.getBytes());
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
    String numericKey = key.replace("user", "");

    try {
      //URL url = URI.create(buildKey(table, numericKey)).toURL();
      URL url = URI.create(endpoint + "/" + numericKey).toURL();
      //System.out.printf("Delete %s\n", url);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");

      String value = "";
      try (OutputStream os = conn.getOutputStream()) {
        os.write(value.getBytes());
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
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String numericKey = key.replace("user", "");

    try {
      //URL url = URI.create(buildKey(table, numericKey)).toURL();
      URL url = URI.create(endpoint + "/" + numericKey).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");

      /*
      ByteIterator byteIterator = values.get("data");
      if (byteIterator == null) {
        System.out.printf("Update error: byteIterator is null\n");
        return Status.ERROR;
      }

      String value = byteIterator.toString();
      */
      String value = values.toString();
      //System.out.printf("Update %s: %s\n", url, value);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(value.getBytes());
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
