package site.ycsb.etcd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * 
 * YCSB binding for etcd v3.
 * Client use the official jetcd library to communicate with etcd
 * 
 */
public class EtcdClient extends DB {
  
  private static final String ENDPOINTS_PROPERTY = "etcd.endpoints";
  private static final String DEFAULT_ENDPOINTS = "http://localhost:2379";
  
  private static final String TIMEOUT_PROPERTY = "etcd.timeout";
  private static final long DEFAULT_TIMEOUT_MS = 5000;
  
  private Client client;
  private KV kvClient;
  private long timeoutMs;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    
    // Get endpoints configuration
    String endpoints = props.getProperty(ENDPOINTS_PROPERTY, DEFAULT_ENDPOINTS);
    
    // Get timeout
    String timeoutStr = props.getProperty(TIMEOUT_PROPERTY);
    timeoutMs = timeoutStr != null ? Long.parseLong(timeoutStr) : DEFAULT_TIMEOUT_MS;
    
    try {
      // Build etcd client
      String[] endpointArray = endpoints.split(",");
      ClientBuilder builder = Client.builder();
      for (String endpoint : endpointArray) {
        builder.endpoints(endpoint.trim());
      }
      
      client = builder.build();
      kvClient = client.getKVClient();
      
      System.out.println("Connected to etcd at: " + endpoints);
    } catch (Exception e) {
      throw new DBException("Failed to connect to etcd: " + e.getMessage(), e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        throw new DBException("Failed to close etcd connection: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String etcdKey = buildKey(table, key);
    
    try {
      ByteSequence keySeq = ByteSequence.from(etcdKey, StandardCharsets.UTF_8);
      CompletableFuture<GetResponse> future = kvClient.get(keySeq);
      GetResponse response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
      
      if (response.getKvs().isEmpty()) {
        return Status.NOT_FOUND;
      }
      
      KeyValue kv = response.getKvs().get(0);
      byte[] value = kv.getValue().getBytes();
      
      deserializeFields(value, fields, result);
      return Status.OK;
      
    } catch (TimeoutException e) {
      System.err.println("Timeout reading key: " + etcdKey);
      return Status.SERVICE_UNAVAILABLE;
    } catch (InterruptedException | ExecutionException e) {
      System.err.println("Error reading key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    } catch (IOException e) {
      System.err.println("Error deserializing data for key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    String etcdKey = buildKey(table, key);
    
    try {
      byte[] data = serializeFields(values);
      
      ByteSequence keySeq = ByteSequence.from(etcdKey, StandardCharsets.UTF_8);
      ByteSequence valueSeq = ByteSequence.from(data);
      
      CompletableFuture<PutResponse> future = kvClient.put(keySeq, valueSeq);
      future.get(timeoutMs, TimeUnit.MILLISECONDS);
      
      return Status.OK;
      
    } catch (TimeoutException e) {
      System.err.println("Timeout inserting key: " + etcdKey);
      return Status.SERVICE_UNAVAILABLE;
    } catch (InterruptedException | ExecutionException e) {
      System.err.println("Error inserting key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    } catch (IOException e) {
      System.err.println("Error serializing data for key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    String etcdKey = buildKey(table, key);
    
    try {
      ByteSequence keySeq = ByteSequence.from(etcdKey, StandardCharsets.UTF_8);
      
      CompletableFuture<GetResponse> getFuture = kvClient.get(keySeq);
      GetResponse getResponse = getFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
      
      if (getResponse.getKvs().isEmpty()) {
        return Status.NOT_FOUND;
      }
      
      KeyValue kv = getResponse.getKvs().get(0);
      byte[] existingData = kv.getValue().getBytes();
      Map<String, ByteIterator> existingValues = new HashMap<>();
      deserializeFields(existingData, null, existingValues);

      existingValues.putAll(values);
      
      byte[] newData = serializeFields(existingValues);
      ByteSequence valueSeq = ByteSequence.from(newData);
      
      CompletableFuture<PutResponse> putFuture = kvClient.put(keySeq, valueSeq);
      putFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
      
      return Status.OK;
      
    } catch (TimeoutException e) {
      System.err.println("Timeout updating key: " + etcdKey);
      return Status.SERVICE_UNAVAILABLE;
    } catch (InterruptedException | ExecutionException e) {
      System.err.println("Error updating key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    } catch (IOException e) {
      System.err.println("Error processing data for key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    String etcdKey = buildKey(table, key);
    
    try {
      ByteSequence keySeq = ByteSequence.from(etcdKey, StandardCharsets.UTF_8);
      
      CompletableFuture<DeleteResponse> future = kvClient.delete(keySeq);
      future.get(timeoutMs, TimeUnit.MILLISECONDS);
      
      return Status.OK;
      
    } catch (TimeoutException e) {
      System.err.println("Timeout deleting key: " + etcdKey);
      return Status.SERVICE_UNAVAILABLE;
    } catch (InterruptedException | ExecutionException e) {
      System.err.println("Error deleting key: " + etcdKey + ": " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  private String buildKey(String table, String key) {
    if (table != null && !table.isEmpty()) {
      return "/" + table + "/" + key;
    } else {
      return "/" + key;
    }
  }

  private byte[] serializeFields(Map<String, ByteIterator> values) throws IOException {
    Properties props = new Properties();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue().toString());
    }
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    props.store(out, null);
    return out.toByteArray();
  }

  private void deserializeFields(byte[] data, Set<String> fields,
                                 Map<String, ByteIterator> result) throws IOException {
    Properties props = new Properties();
    props.load(new ByteArrayInputStream(data));
    
    for (String key : props.stringPropertyNames()) {
      if (fields == null || fields.contains(key)) {
        result.put(key, new StringByteIterator(props.getProperty(key)));
      }
    }
  }
}
