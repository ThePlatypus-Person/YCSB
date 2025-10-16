package site.ycsb.tikv;

import site.ycsb.*;
import java.util.*;

import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.Optional;

public class TikvClient extends DB {
  private static final String CLIENT_CONNECT = "tikv.clientConnect";
  private static final String DEFAULT_CLIENT_CONNECT = "127.0.0.1:2379";

  private TiSession session;
  private RawKVClient client;

  public TikvClient() {
    System.out.println(">>> Using TikvClient!");
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String connectString = props.getProperty(CLIENT_CONNECT);
    if (connectString == null || connectString.length() == 0) {
      connectString = DEFAULT_CLIENT_CONNECT;
    }

    TiConfiguration conf = TiConfiguration.createDefault(connectString);
    this.session = TiSession.create(conf);
    this.client = session.createRawClient();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Optional<ByteString> valueOpt = client.get(ByteString.copyFromUtf8(key));

      if (valueOpt.isPresent()) {
        //System.out.printf("Read key %s = %s\n", key, valueOpt.get().toStringUtf8());
        return Status.OK;
      } else {
        //System.out.printf("Read key %s FAIL\n", key);
        return Status.ERROR;
      }

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }


  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      String value = values.toString();
      //System.out.printf("Insert key %s = %s\n", key, value);
      client.put(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      //System.out.printf("DELETE key %s\n", key);
      client.delete(ByteString.copyFromUtf8(key));
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      String value = values.toString();
      //System.out.printf("Update key %s = %s\n", key, value);
      client.put(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startKey, int recordCount,
    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    System.out.println("ERROR: scan() is not implemented");
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public void cleanup() throws DBException {
    System.out.println(">>> Cleaning up TikvClient");

    try {
      if (client != null) {
        client.close();
      }
      if (session != null) {
        session.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new DBException("Error while closing Tikv client", e);
    }
  }
}
