package site.ycsb.db;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
  private static HttpClient sharedClient;
  private static String[] hosts;
  private static final AtomicInteger HOST_INDEX = new AtomicInteger(0);
  private boolean debug = false;

  @Override
  public void init() throws DBException {
    synchronized (THREAD_COUNT) {
      if (sharedClient == null) {
        Properties props = getProperties();
        String hostsString = props.getProperty(HOSTS_PROPERTY);
        if (hostsString == null) {
          throw new DBException("Required property '" + HOSTS_PROPERTY + "' is missing.");
        }
        hosts = hostsString.split(",");
        sharedClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .build();
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
          sharedClient = null;
          if (this.debug) {
            System.out.println("Last hraftd thread cleaned up. Shared client released.");
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
    String url = getNextHost() + "/key/" + key;
    if (debug) {
      System.out.println("GET: " + url);
    }

    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .GET()
          .build();
      
      HttpResponse<String> response = sharedClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        result.put("value", new StringByteIterator(response.body()));
        return Status.OK;
      } else if (response.statusCode() == 404) {
        return Status.NOT_FOUND;
      }
      
      if (debug) {
        System.err.println("Read failed with status code: " + response.statusCode());
      }
      return Status.ERROR;
    } catch (IOException | InterruptedException e) {
      if (debug) {
        System.err.println("Read failed for " + url + " - " + e.getMessage());
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
    
    String url = getNextHost() + "/key";
    
    if (debug) {
      System.out.println("POST: " + url + " Body: " + jsonBody);
    }

    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
          .header("Content-Type", "application/json")
          .build();

      HttpResponse<String> response = sharedClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() == 200) {
        return Status.OK;
      }
      
      if (debug) {
        System.err.println("Write failed with status code: " + response.statusCode() + 
                          " Body: " + response.body());
      }
      return Status.ERROR;
    } catch (IOException | InterruptedException e) {
      if (debug) {
        System.err.println("Write failed for " + url + " - " + e.getMessage());
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    String url = getNextHost() + "/key/" + key;
    if (debug) {
      System.out.println("DELETE: " + url);
    }

    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .DELETE()
          .build();

      HttpResponse<String> response = sharedClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() == 200) {
        return Status.OK;
      }
      
      if (debug) {
        System.err.println("Delete failed with status code: " + response.statusCode());
      }
      return Status.ERROR;
    } catch (IOException | InterruptedException e) {
      if (debug) {
        System.err.println("Delete failed for " + url + " - " + e.getMessage());
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