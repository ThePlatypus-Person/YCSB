package site.ycsb.holipaxos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * YCSB binding for HoliPaxos consensus protocol.
 * 
 */
public class HolipaxosClient extends DB {
  
  private static final String HOSTS_PROPERTY = "holipaxos.hosts";
  private static final String DEFAULT_HOSTS = "127.0.0.1:2001,127.0.0.1:2211,127.0.0.1:2221";
  private static final String TIMEOUT_PROPERTY = "holipaxos.timeout";
  private static final int DEFAULT_TIMEOUT_MS = 5000;
  
  private String[] hosts;
  private int timeoutMs;
  private int currentHost;
  
  private Socket socket;
  private PrintWriter out;
  private BufferedReader in;
  
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    
    String hostsStr = props.getProperty(HOSTS_PROPERTY, DEFAULT_HOSTS);
    hosts = hostsStr.split(",");
    for (int i = 0; i < hosts.length; i++) {
      hosts[i] = hosts[i].trim();
    }
    
    String timeoutStr = props.getProperty(TIMEOUT_PROPERTY);
    timeoutMs = timeoutStr != null ? Integer.parseInt(timeoutStr) : DEFAULT_TIMEOUT_MS;
    
    currentHost = ThreadLocalRandom.current().nextInt(hosts.length);
    
    connect();
  }
  
  @Override
  public void cleanup() throws DBException {
    System.out.println("cleanup() called");
    if (socket != null && !socket.isClosed()) {
      try {
        socket.close();
      } catch (IOException e) {
        // Best effort close
      }
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields, 
                     Map<String, ByteIterator> result) {
    //System.out.println("read() called with key: " + key);
    try {
      String response = executeWithRetry("get " + key);
      
      if (response.equals("key not found")) {
        return Status.NOT_FOUND;
      }
      
      String decodedValue = URLDecoder.decode(response, StandardCharsets.UTF_8.name());
      result.put("field0", new StringByteIterator(decodedValue));
      return Status.OK;
      
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    //System.out.println("insert() called with key: " + key);
    StringBuilder value = new StringBuilder();
    for (ByteIterator v : values.values()) {
      // Don't add spaces between field values to avoid parser issues
      value.append(v.toString());
    }
    
    try {
      // URL encode the value to avoid any parsing issues with special characters
      String encodedValue = URLEncoder.encode(value.toString(), StandardCharsets.UTF_8.name());
      executeWriteWithRetry("put " + key + " " + encodedValue);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    //System.out.println("update() called with key: " + key);
    return insert(table, key, values);
  }
  
  @Override
  public Status delete(String table, String key) {
    //System.out.println("delete() called with key: " + key);
    try {
      executeWriteWithRetry("del " + key);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  
  @Override
  public Status scan(String table, String startkey, int recordcount, 
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
  
  private void connect() throws DBException {

    System.out.println("Flag 1 : Connecting to HoliPaxos node " + currentHost);
    if (socket != null && !socket.isClosed()) {
      System.out.println("Closing existing socket");
      try {
        socket.close();
      } catch (IOException e) {
        // Ignore
      }
    }
    
    System.out.println("Flag 2 : Connecting to HoliPaxos node " + currentHost);
    for (int i = 0; i < hosts.length; i++) {
      String[] parts = hosts[currentHost].split(":");
      String host = parts[0];
      int port = Integer.parseInt(parts[1]);
      
      System.out.println("Trying to connect to " + host + ":" + port);
      try {
        socket = new Socket(host, port);
        socket.setSoTimeout(timeoutMs);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // System.out.println("Connected to HoliPaxos node " + currentHost);
        return;
      } catch (IOException e) {
        currentHost = (currentHost + 1) % hosts.length;
        System.out.println("Failed to connect to " + host + ":" + port + ", trying next node");
      }
    }

    System.out.println("Failed to connect to any HoliPaxos node");
    throw new DBException("Cannot connect to any HoliPaxos node");
  }
  
  private String executeWithRetry(String command) throws Exception {
    int maxRetries = 3;
    Exception lastException = null;
    
    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        return execute(command);
      } catch (Exception e) {
        lastException = e;
        // Try next server
        currentHost = (currentHost + 1) % hosts.length;
        try {
          connect();
        } catch (DBException ce) {
          // Continue with retry
        }
      }
    }
    
    throw lastException != null ? lastException : new Exception("Failed after " + maxRetries + " retries");
  }
  
  private void executeWriteWithRetry(String command) throws Exception {
    int maxRetries = 3;
    Exception lastException = null;
    
    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        executeWrite(command);
        return;
      } catch (Exception e) {
        lastException = e;
        // Try next server
        currentHost = (currentHost + 1) % hosts.length;
        try {
          connect();
        } catch (DBException ce) {
          // Continue with retry
        }
      }
    }
    
    throw lastException != null ? lastException : new Exception("Failed after " + maxRetries + " retries");
  }
  
  private String execute(String command) throws Exception {
    //out.println(command);
    //out.flush();
    
    String response = in.readLine();
    if (response == null) {
      throw new IOException("Connection closed");
    }
    
    response = response.trim();
    
    if (response.equals("retry")) {
      throw new Exception("Server busy");
    }
    
    if (response.startsWith("leader is ")) {
      handleLeaderRedirect(response);
      // Retry with new leader
      return execute(command);
    }
    
    if (response.equals("bad command")) {
      throw new Exception("Invalid command");
    }
    
    return response;
  }
  
  private void executeWrite(String command) throws Exception {
    int originalTimeout = timeoutMs;
    
    try {
      socket.setSoTimeout(500);
      //out.println(command);
      //out.flush();
      
      try {
        String response = in.readLine();
        
        if (response != null) {
          response = response.trim();
          
          if (response.isEmpty()) {
            // Empty response = success for writes
            return;
          }
          
          if (response.equals("retry")) {
            throw new Exception("Server busy");
          }
          
          if (response.startsWith("leader is ")) {
            handleLeaderRedirect(response);
            // Retry with new leader
            executeWrite(command);
            return;
          }
          
          if (response.equals("bad command")) {
            throw new Exception("Invalid command");
          }
        }
      } catch (SocketTimeoutException e) {
        // Timeout = success for writes (no response expected)
      }
      
    } finally {
      // Restore original timeout
      socket.setSoTimeout(originalTimeout);
    }
  }
  
  private void handleLeaderRedirect(String response) throws DBException {
    String[] parts = response.split(" ");
    if (parts.length >= 3) {
      try {
        int leaderId = Integer.parseInt(parts[2]);
        if (leaderId >= 0 && leaderId < hosts.length) {
          if (leaderId != currentHost) {
            currentHost = leaderId;
            connect();
          }
        }
      } catch (NumberFormatException e) {
        // Invalid leader ID, ignore
      }
    }
  }
}
