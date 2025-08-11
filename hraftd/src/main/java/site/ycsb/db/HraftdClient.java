package site.ycsb.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * YCSB database binding for hraftd, a Raft-based key-value store.
 */
public class HraftdClient extends DB {
  private static final String HOSTS_PROPERTY = "hraftd.hosts";
  private static final String DEBUG_PROPERTY = "debug";
  
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);
  private static String[] hosts;
  private static final AtomicInteger HOST_INDEX = new AtomicInteger(0);
  private boolean debug = false;

  @Override
  public void init() throws DBException {
    synchronized (THREAD_COUNT) {
      if (hosts == null) {
        Properties props = getProperties();
        String hostsString = props.getProperty(HOSTS_PROPERTY);
        if (hostsString == null) {
          throw new DBException("Required property '" + HOSTS_PROPERTY + "' is missing.");
        }
        hosts = hostsString.split(",");
        this.debug = props.getProperty(DEBUG_PROPERTY, "false").equals("true");
        if (this.debug) {
          System.out.println("hraftd client initialized. Hosts: " + hostsString);
        }
      }
    }
    THREAD_COUNT.incrementAndGet();
  }

  @Override
  public void cleanup() throws DBException {
    if (THREAD_COUNT.decrementAndGet() <= 0) {
      synchronized(THREAD_COUNT) {
        if (THREAD_COUNT.get() <= 0) {
          hosts = null;
          if (this.debug) {
            System.out.println("Last hraftd thread cleaned up.");
          }
        }
      }
    }
  }

  private String getNextHost() {
    int index = HOST_INDEX.getAndIncrement() % hosts.length;
    return hosts[index];
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String urlString = getNextHost() + "/key/" + key;
    if (debug) {
      System.out.println("GET: " + urlString);
    }

    try {
      URL url = new URL(urlString);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.setInstanceFollowRedirects(true);
      
      int responseCode = connection.getResponseCode();
      
      if (responseCode == 200) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
          StringBuilder response = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line);
          }
          
          result.put("value", new StringByteIterator(response.toString()));
          return Status.OK;
        }
      } else if (responseCode == 404) {
        return Status.NOT_FOUND;
      }
      
      if (debug) {
        System.err.println("Read failed with status code: " + responseCode);
      }
      return Status.ERROR;
    } catch (IOException e) {
      if (debug) {
        System.err.println("Read failed for " + urlString + " - " + e.getMessage());
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return writeKeyValue(key, values);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return writeKeyValue(key, values);
  }

  private Status writeKeyValue(String key, Map<String, ByteIterator> values) {
    String serializedValue = serializeFields(values);
    
    String jsonBody = "{\"" + escapeJson(key) + "\":\"" + escapeJson(serializedValue) + "\"}";
    
    String urlString = getNextHost() + "/key";
    
    if (debug) {
      System.out.println("POST: " + urlString + " Body: " + jsonBody);
    }

    try {
      URL url = new URL(urlString);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);
      connection.setInstanceFollowRedirects(true);

      try (OutputStream os = connection.getOutputStream()) {
        os.write(jsonBody.getBytes("UTF-8"));
      }
      
      int responseCode = connection.getResponseCode();
      
      if (responseCode == 200) {
        return Status.OK;
      }
      
      if (debug) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream()))) {
          StringBuilder response = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line);
          }
          
          System.err.println("Write failed with status code: " + responseCode + 
                            " Body: " + response.toString());
        }
      }
      return Status.ERROR;
    } catch (IOException e) {
      if (debug) {
        System.err.println("Write failed for " + urlString + " - " + e.getMessage());
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    String urlString = getNextHost() + "/key/" + key;
    if (debug) {
      System.out.println("DELETE: " + urlString);
    }

    try {
      URL url = new URL(urlString);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("DELETE");
      connection.setInstanceFollowRedirects(true);
      
      int responseCode = connection.getResponseCode();
      
      if (responseCode == 200) {
        return Status.OK;
      }
      
      if (debug) {
        System.err.println("Delete failed with status code: " + responseCode);
      }
      return Status.ERROR;
    } catch (IOException e) {
      if (debug) {
        System.err.println("Delete failed for " + urlString + " - " + e.getMessage());
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  private String serializeFields(Map<String, ByteIterator> values) {
    if (values.isEmpty()) {
      return "{}";
    }
    
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean first = true;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      first = false;
      sb.append("\"").append(escapeJson(entry.getKey())).append("\":\"");
      sb.append(escapeJson(entry.getValue().toString())).append("\"");
    }
    sb.append("}");
    return sb.toString();
  }

  private String escapeJson(String str) {
    return str.replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replace("\b", "\\b")
              .replace("\f", "\\f")
              .replace("\n", "\\n")
              .replace("\r", "\\r")
              .replace("\t", "\\t");
  }
}