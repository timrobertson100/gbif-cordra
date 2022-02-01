package org.gbif.cordra.storage.hbase;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import net.cnri.cordra.api.*;
import net.cnri.cordra.collections.AbstractSearchResults;
import net.cnri.cordra.storage.CordraStorage;
import net.cnri.cordra.util.GsonUtility;
import net.cnri.cordra.util.JsonUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HBase Storage Handler for Cordra that uses row key prefix salting for a table with partitions
 * balanced by row count.
 */
public class HBaseStorage implements CordraStorage {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseStorage.class);
  private static final Gson gson = GsonUtility.getGson();
  private static final byte[] CF_METADATA = Bytes.toBytes("m");
  private static final byte[] COL_METADATA = Bytes.toBytes("m");

  private Connection hbaseConnection;
  private TableName tableName;
  private ModulusSalt salt;

  public HBaseStorage(JsonObject options) throws IOException {
    this(
        JsonUtil.getAsStringOrNull(options, "tableName"),
        JsonUtil.getAsStringOrNull(options, "saltModulus"),
        JsonUtil.getAsStringOrNull(options, "zk"));
  }

  public HBaseStorage(String tableAsString, String saltAsString, String zk) throws IOException {
    tableName = TableName.valueOf(tableAsString);
    salt = new ModulusSalt(Integer.parseInt(saltAsString));
    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", zk);
    hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
  }

  @Override
  public CordraObject get(String s) throws CordraException {
    byte[] key = getHBaseKeyIdFor(s);
    try (Table table = hbaseConnection.getTable(tableName)) {
      Get get = new Get(key);
      get.addColumn(CF_METADATA, COL_METADATA);
      Result result = table.get(get);
      if (result.isEmpty() || result.getValue(CF_METADATA, COL_METADATA) == null) {
        return null;
      } else {
        byte[] bytes = result.getValue(CF_METADATA, COL_METADATA);
        return gson.fromJson(Bytes.toString(bytes), CordraObject.class);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new InternalErrorCordraException(e);
    }
  }

  private boolean doesHBaseObjectExist(byte[] key) throws CordraException {
    try (Table table = hbaseConnection.getTable(tableName)) {
      Get get = new Get(key);
      get.addColumn(CF_METADATA, COL_METADATA);
      return table.exists(get);
    } catch (IOException e) {
      throw new InternalErrorCordraException(e);
    }
  }

  @Override
  public InputStream getPayload(String id, String payloadName) throws CordraException {
    throw new IllegalArgumentException();
  }

  @Override
  public InputStream getPartialPayload(String id, String payloadName, Long start, Long end)
      throws CordraException {
    throw new IllegalArgumentException();
  }

  @Override
  public CordraObject create(CordraObject d) throws CordraException {
    byte[] key = getHBaseKeyIdFor(d.id);
    if (doesHBaseObjectExist(key)) {
      throw new ConflictCordraException("Object already exists: " + d.id);
    }
    if (d.payloads != null) {
      // TODO
    }

    writeToHBase(key, d);
    return d;
  }

  private void writeToHBase(byte[] key, CordraObject d) throws InternalErrorCordraException {
    String json = gson.toJson(d);
    try (Table table = hbaseConnection.getTable(tableName)) {
      Put put = new Put(key);
      put.addColumn(CF_METADATA, COL_METADATA, Bytes.toBytes(json));
      table.put(put);
    } catch (IOException e) {
      throw new InternalErrorCordraException(e);
    }
  }

  @Override
  public CordraObject update(CordraObject d) throws CordraException {
    byte[] key = getHBaseKeyIdFor(d.id);
    if (!doesHBaseObjectExist(key)) {
      throw new NotFoundCordraException("Missing object: " + d.id);
    }
    List<String> payloadsToDelete = d.getPayloadsToDelete();
    String id = d.id;
    if (payloadsToDelete != null) {
      for (String payloadName : payloadsToDelete) {
        // TODO
      }
    }
    d.clearPayloadsToDelete();
    if (d.payloads != null) {
      for (Payload p : d.payloads) {
        // TODO
      }
      if (d.payloads.isEmpty()) {
        d.payloads = null;
      }
    }
    writeToHBase(key, d);
    return d;
  }

  @Override
  public void delete(String id) throws CordraException {
    byte[] key = getHBaseKeyIdFor(id);
    if (!doesHBaseObjectExist(key)) {
      throw new NotFoundCordraException("Missing object: " + id);
    }
    CordraObject d = this.get(id);
    List<Delete> cellsToDelete = Lists.newArrayList();
    try (Table table = hbaseConnection.getTable(tableName)) {
      Delete delete = new Delete(key);
      delete.addColumn(CF_METADATA, COL_METADATA);
      cellsToDelete.add(delete);

      if (d.payloads != null) {
        for (Payload p : d.payloads) {
          // TODO add cells for each payload
        }
      }

      table.delete(cellsToDelete);
    } catch (IOException e) {
      throw new InternalErrorCordraException(e);
    }
  }

  @Override
  public SearchResults<CordraObject> list() throws CordraException {
    try {
      return new HBaseListSearchResults();
    } catch (IOException e) {
      throw new InternalErrorCordraException(e);
    }
  }

  @Override
  public SearchResults<String> listHandles() throws CordraException {
    try {
      return new HBaseListHandlesSearchResults();
    } catch (IOException e) {
      throw new InternalErrorCordraException(e);
    }
  }

  @Override
  public void close() {
    try {
      hbaseConnection.close();
    } catch (IOException ex) {
      LOG.error("Can't close HBase connection", ex);
    }
  }

  // Applies salting to get the true HBase row key
  private byte[] getHBaseKeyIdFor(String id) {
    return salt.salt(id);
  }

  private class HBaseListSearchResults extends AbstractSearchResults<CordraObject> {
    private Iterator<Result> iter;

    public HBaseListSearchResults() throws IOException {
      Scan scan = new Scan();
      scan.addColumn(CF_METADATA, COL_METADATA);
      Table table = hbaseConnection.getTable(tableName);
      iter = table.getScanner(scan).iterator();
    }

    @Override
    public int size() {
      return -1;
    }

    @Override
    protected CordraObject computeNext() {
      if (iter.hasNext()) {
        Result result = iter.next();
        byte[] bytes = result.getValue(CF_METADATA, COL_METADATA);
        return gson.fromJson(Bytes.toString(bytes), CordraObject.class);
      } else {
        return null;
      }
    }
  }

  private class HBaseListHandlesSearchResults extends AbstractSearchResults<String> {
    private Iterator<Result> iter;

    public HBaseListHandlesSearchResults() throws IOException {
      Scan scan = new Scan();
      scan.setFilter(new FirstKeyOnlyFilter()); // row keys only
      Table table = hbaseConnection.getTable(tableName);
      iter = table.getScanner(scan).iterator();
    }

    @Override
    public int size() {
      return -1;
    }

    @Override
    protected String computeNext() {
      if (iter.hasNext()) {
        Result result = iter.next();
        byte[] rowKey = result.getRow();
        return salt.idFrom(rowKey);
      } else {
        return null;
      }
    }
  }
}
