package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.*;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AccumuloSession extends ModelSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSession.class);

    private static final String ACCUMULO_INSTANCE_NAME = "bigtable.accumulo.instanceName";
    private static final String ACCUMULO_USER = "bigtable.accumulo.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.accumulo.password";
    private static final String ZK_SERVER_NAMES = "bigtable.accumulo.zookeeperServerNames";

    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;

    private Connector connector;
    private long maxMemory = 1000000L;
    private long maxLatency = 1000L;
    private int maxWriteThreads = 10;

    @Override
    public void init(Map<String, Object> properties) {
        LOGGER.trace("init called with parameters: properties=?", properties);
        checkProperties(properties);
        try {
            String zkServerNames;
            if (properties.get(ZK_SERVER_NAMES) instanceof Collection) {
                zkServerNames = StringUtils.join((Collection) properties.get(ZK_SERVER_NAMES), ",");
            } else {
                zkServerNames = (String) properties.get(ZK_SERVER_NAMES);
            }
            ZooKeeperInstance zk = new ZooKeeperInstance((String) properties.get(ACCUMULO_INSTANCE_NAME), zkServerNames);

            String username = (String) properties.get(ACCUMULO_USER);
            String password = (String) properties.get(ACCUMULO_PASSWORD);
            this.connector = zk.getConnector(username, new PasswordToken(password));
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
    public void save(Row row, ModelUserContext user) {
        LOGGER.trace("save called with parameters: row=?, user=?", row, user);
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
    public void saveMany(String tableName, Collection<Row> rows, ModelUserContext user) {
        LOGGER.trace("saveMany called with parameters: tableName=?, rows=?, user=?", tableName, rows, user);
        if (rows.size() == 0) {
            return;
        }
        try {
            BatchWriter writer = connector.createBatchWriter(tableName, getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
            for (Row row : rows) {
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
    public List<Row> findByRowKeyRange(String tableName, String rowKeyStart, String rowKeyEnd, ModelUserContext user) {
        LOGGER.trace("findByRowKeyRange called with parameters: tableName=?, rowKeyStart=?, rowKeyEnd=?, user=?", tableName, rowKeyStart, rowKeyEnd, user);
        try {
            Scanner scanner = createScanner(tableName, user);
            if (rowKeyStart != null) {
                scanner.setRange(new Range(rowKeyStart, rowKeyEnd));
            }
            return AccumuloHelper.scannerToRows(tableName, scanner);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Row> findByRowStartsWith(String tableName, String rowKeyPrefix, ModelUserContext user) {
        return findByRowKeyRange(tableName, rowKeyPrefix, rowKeyPrefix + "ZZZZ", user); // TODO is this the best way?
    }

    @Override
    public List<Row> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user) {
        LOGGER.trace("findByRowKeyRegex called with parameters: tableName=?, rowKeyRegex=?, user=?", tableName, rowKeyRegex, user);
        try {
            Scanner scanner = createScanner(tableName, user);
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
    public Iterable<Row> findAll(String tableName, ModelUserContext user) {
        LOGGER.trace("findAll called with parameters: tableName=?, user=?", tableName, user);
        try {
            Scanner scanner = createScanner(tableName, user);
            return AccumuloHelper.scannerToRowsIterable(tableName, scanner);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long rowCount(String tableName, ModelUserContext user) {
        LOGGER.trace("rowCount called with parameters: tableName=?, user=?", tableName, user);
        try {
            // TODO this requires all rows to be returned to the client. It would be nice to have this run server side.
            Scanner scanner = createScanner(tableName, user);
            RowIterator rowIterator = new RowIterator(scanner);
            long count = 0;
            while (rowIterator.hasNext()) {
                rowIterator.next();
                count++;
            }
            return count;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row findByRowKey(String tableName, String rowKey, ModelUserContext user) {
        LOGGER.trace("findByRowKey called with parameters: tableName=?, rowKey=?, user=?", tableName, rowKey, user);
        try {
            Scanner scanner = createScanner(tableName, user);
            scanner.setRange(new Range(rowKey));
            List<Row> rows = AccumuloHelper.scannerToRows(tableName, scanner);
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

    private Scanner createScanner(String tableName, ModelUserContext user) throws TableNotFoundException {
        IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
        try {
            if (!this.connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                this.connector.tableOperations().attachIterator(tableName, is);
            }

//            return this.connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Scanner scanner = this.connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
        IteratorSetting iteratorSetting = new IteratorSetting(
                100,
                RowDeletingIterator.class.getSimpleName(),
                RowDeletingIterator.class
        );
        scanner.addScanIterator(iteratorSetting);
        return scanner;
    }

    @Override
    public Row findByRowKey(String tableName, String rowKey, Map<String, String> columnsToReturn, ModelUserContext user) {
        LOGGER.trace("findByRowKey called with parameters: tableName=?, rowKey=?, columnsToReturn=?, user=?", tableName, rowKey, columnsToReturn, user);
        try {
            Scanner scanner = createScanner(tableName, user);
            scanner.setRange(new Range(rowKey));
            for (Map.Entry<String, String> columnFamilyAndColumnQualifier : columnsToReturn.entrySet()) {
                if (columnFamilyAndColumnQualifier.getValue().equals("*")) {
                    scanner.fetchColumnFamily(new Text(columnFamilyAndColumnQualifier.getKey()));
                } else {
                    scanner.fetchColumn(new Text(columnFamilyAndColumnQualifier.getKey()), new Text(columnFamilyAndColumnQualifier.getValue()));
                }
            }
            List<Row> rows = AccumuloHelper.scannerToRows(tableName, scanner);
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
        LOGGER.trace("initializeTable called with parameters: tableName=?, user=?", tableName, user);
        LOGGER.debug("initializeTable: " + tableName);
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
        LOGGER.trace("deleteTable called with parameters: tableName=?, user=?", tableName, user);
        LOGGER.debug("deleteTable: " + tableName);
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
        LOGGER.trace("deleteRow called with parameters: tableName=?, rowKey=?, user=?", tableName, rowKey, user);
        // In most instances (e.g., when reading is not necessary), the
        // RowDeletingIterator gives better performance than the deleting
        // mutation. This is due to the fact that Deleting mutations marks each
        // entry with a delete marker. Using the iterator marks a whole row with
        // a single mutation.
        try {
            BatchWriter writer = connector.createBatchWriter(tableName, getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
            try {
                Mutation mutation = new Mutation(rowKey.toString());
                mutation.put("", "", RowDeletingIterator.DELETE_ROW_VALUE);
                writer.flush();
            } catch (AccumuloException ae) {
                throw new RuntimeException(ae);
            } finally {
                writer.close();
            }
        } catch (MutationsRejectedException mre) {
            throw new RuntimeException(mre);
        } catch (TableNotFoundException tnfe) {
            throw new RuntimeException(tnfe);
        }
    }

    @Override
    public void deleteColumn(Row row, String tableName, String columnFamily, String columnQualifier, ModelUserContext user) {
        LOGGER.trace("deleteColumn called with parameters: row=?, tableName=?, columnFamily=?, columnQualifier=?, user=?", row, tableName, columnFamily, columnQualifier, user);
        try {
            BatchWriter writer = connector.createBatchWriter(tableName, getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
            Mutation mutation = createMutationFromRow(row);
            mutation.putDelete(new Text(columnFamily), new Text(columnQualifier));
            writer.addMutation(mutation);
            writer.flush();
            connector.tableOperations().flush(tableName, null, null, true);
            writer.close();
        } catch (AccumuloException ae) {
            throw new RuntimeException(ae);
        } catch (AccumuloSecurityException ase) {
            throw new RuntimeException(ase);
        } catch (TableNotFoundException tne) {
            throw new RuntimeException(tne);
        }
    }

    @Override
    public List<String> getTableList(ModelUserContext user) {
        LOGGER.trace("getTableList called with parameters: user=?", user);
        return new ArrayList<String>(this.connector.tableOperations().list());
    }

    @Override
    public void close() {
        //Accumulo a persistent connection object, so this is unnecessary
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

    public static Mutation createMutationFromRow(Row row) {
        LOGGER.trace("createMutationFromRow called with parameters: row=?", row);
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

    private void checkProperties(Map<String, Object> properties) {
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
