package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.*;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AccumuloSession extends ModelSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSession.class.getName());

    private static final String ACCUMULO_INSTANCE_NAME = "bigtable.accumulo.instanceName";
    private static final String ACCUMULO_USER = "bigtable.accumulo.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.accumulo.password";
    private static final String ZK_SERVER_NAMES = "bigtable.accumulo.zookeeperServerNames";

    private Connector connector;
    private long maxMemory = 1000000L;
    private long maxLatency = 1000L;
    private int maxWriteThreads = 10;

    @Override
    public void init(Map<String, String> properties) {
		Map<String, String> checkedProperties = properties;
    	
    	checkProperties(checkedProperties);
        try {
            ZooKeeperInstance zk = new ZooKeeperInstance((String)properties.get(ACCUMULO_INSTANCE_NAME), (String)properties.get(ZK_SERVER_NAMES));
            this.connector = zk.getConnector((String)properties.get(ACCUMULO_USER), ((String)properties.get(ACCUMULO_PASSWORD)).getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public AccumuloSession() {

    }

    public AccumuloSession(Connector connector) {
        this.connector = connector;
    }

    @Override
    public void save(Row<? extends RowKey> row, ModelUserContext user) {
        try {
            BatchWriter writer = connector.createBatchWriter(row.getTableName(), getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
            AccumuloHelper.addRowToWriter(writer, row);
            writer.flush();
            writer.close();
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveMany(String tableName, Collection<Row<? extends RowKey>> rows, ModelUserContext user) {
        if (rows.size() == 0) {
            return;
        }
        try {
            BatchWriter writer = connector.createBatchWriter(tableName, getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
            for (Row<? extends RowKey> row : rows) {
                AccumuloHelper.addRowToWriter(writer, row);
            }
            writer.flush();
            writer.close();

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Row<? extends RowKey>> findByRowKeyRange(String tableName, String rowKeyStart, String rowKeyEnd, ModelUserContext user) {
        try {
            Scanner scanner = this.connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
            if (rowKeyStart != null) {
                scanner.setRange(new Range(rowKeyStart, rowKeyEnd));
            }
            return AccumuloHelper.scannerToRows(tableName, scanner);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Row<? extends RowKey>> findByRowStartsWith(String tableName, String rowKeyPrefix, ModelUserContext user) {
        return findByRowKeyRange(tableName, rowKeyPrefix, rowKeyPrefix + "ZZZZ", user); // TODO is this the best way?
    }

    @Override
    public List<Row<? extends RowKey>> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user) {
        try {
            Scanner scanner = this.connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
            scanner.setRange(new Range());

            IteratorSetting iter = new IteratorSetting(15, "regExFilter", RegExFilter.class);
            RegExFilter.setRegexs(iter, rowKeyRegex, null, null, null, false);
            scanner.addScanIterator(iter);

            return AccumuloHelper.scannerToRows(tableName, scanner);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row<? extends RowKey> findByRowKey(String tableName, String rowKey, ModelUserContext user) {
        try {
            Scanner scanner = this.connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
            scanner.setRange(new Range(rowKey));
            List<Row<? extends RowKey>> rows = AccumuloHelper.scannerToRows(tableName, scanner);
            if (rows.size() == 0) {
                return null;
            }
            if (rows.size() > 1) {
                throw new RuntimeException("Too many rows returned for a single row query (rowKey: " + rowKey + ", size: " + rows.size() + ")");
            }
            return rows.get(0);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row<? extends RowKey> findByRowKey(String tableName, String rowKey, Map<String, String> columnsToReturn, ModelUserContext user) {
        try {
            Scanner scanner = this.connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
            scanner.setRange(new Range(rowKey));
            for (Map.Entry<String, String> columnFamilyAndColumnQualifier : columnsToReturn.entrySet()) {
                if (columnFamilyAndColumnQualifier.getValue().equals("*")) {
                    scanner.fetchColumnFamily(new Text(columnFamilyAndColumnQualifier.getKey()));
                } else {
                    scanner.fetchColumn(new Text(columnFamilyAndColumnQualifier.getKey()), new Text(columnFamilyAndColumnQualifier.getValue()));
                }
            }
            List<Row<? extends RowKey>> rows = AccumuloHelper.scannerToRows(tableName, scanner);
            if (rows.size() == 0) {
                return null;
            }
            if (rows.size() > 1) {
                throw new RuntimeException("Too many rows returned for a single row query (rowKey: " + rowKey + ", size: " + rows.size() + ")");
            }
            return rows.get(0);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initializeTable(String tableName, ModelUserContext user) {
        LOGGER.info("initializeTable: " + tableName);
        try {
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        } catch (TableExistsException e) {
            throw new RuntimeException(e);
        } catch (AccumuloException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteTable(String tableName, ModelUserContext user) {
        LOGGER.info("deleteTable: " + tableName);
        try {
            if (connector.tableOperations().exists(tableName)) {
                connector.tableOperations().delete(tableName);
            }
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (AccumuloException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteRow(String tableName, RowKey rowKey, ModelUserContext user) {
        LOGGER.info("deleteRow: " + rowKey);
        try {
            // TODO: Find a better way to delete a single row given the row key
            String strRowKey = rowKey.toString();
            char lastChar = strRowKey.charAt(strRowKey.length() - 1);
            char asciiCharBeforeLastChar = (char) (((int) lastChar) - 1);
            String precedingRowKey = strRowKey.substring(0, strRowKey.length() - 1) + asciiCharBeforeLastChar;
            Text startRowKey = new Text(precedingRowKey);
            Text endRowKey = new Text(strRowKey);
            connector.tableOperations().deleteRows(tableName, startRowKey, endRowKey);
        } catch (AccumuloException e) {
            throw new RuntimeException(e);
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteColumn(Row<? extends RowKey> row, String tableName, String columnFamily, String columnQualifier, ModelUserContext user) {
        LOGGER.info("delete column: " + columnQualifier + " from columnFamily: " + columnFamily + ", row: " + row.getRowKey().toString());
        try {
            BatchWriter writer = connector.createBatchWriter(tableName, getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
            Mutation mutation = createMutationFromRow(row);
            mutation.putDelete(new Text(columnFamily), new Text(columnQualifier));
            writer.addMutation(mutation);
            writer.flush();
            writer.close();
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getTableList(ModelUserContext user) {
        return new ArrayList<String>(this.connector.tableOperations().list());
    }

    @Override
    public void close() {
        // TODO: close me
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public long getMaxLatency() {
        return maxLatency;
    }

    public void setMaxLatency(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    public int getMaxWriteThreads() {
        return maxWriteThreads;
    }

    public void setMaxWriteThreads(int maxWriteThreads) {
        this.maxWriteThreads = maxWriteThreads;
    }

    public static Mutation createMutationFromRow(Row<? extends RowKey> row) {
        Mutation mutation = null;
        Collection<ColumnFamily> columnFamilies = row.getColumnFamilies();
        for (ColumnFamily columnFamily : columnFamilies) {
            for (Column column : columnFamily.getColumns()) {
                if (column.isDirty()) {
                    Value value = new Value(column.getValue().toBytes());
                    if (mutation == null) {
                        mutation = new Mutation(row.getRowKey().toString());
                    }
                    mutation.put(columnFamily.getColumnFamilyName(), column.getName(), value);
                }
            }
        }
        return mutation;
    }

    private void checkProperties(Map<String, String> properties) {
        if (properties.get(ACCUMULO_INSTANCE_NAME) == null) {
            throw new IllegalStateException("Configuration property " + ACCUMULO_INSTANCE_NAME + " missing!");
        }

        if (properties.get(ACCUMULO_USER) == null) {
            throw new IllegalStateException("Configuration property " + ACCUMULO_USER + " missing!");
        }

        if (properties.get(ACCUMULO_PASSWORD) == null) {
            throw new IllegalStateException("Configuration property " + ACCUMULO_PASSWORD + " missing!");
        }

        if (properties.get(ZK_SERVER_NAMES) == null) {
            throw new IllegalStateException("Configuration property " + ZK_SERVER_NAMES + " missing!");
        }
    }

}
