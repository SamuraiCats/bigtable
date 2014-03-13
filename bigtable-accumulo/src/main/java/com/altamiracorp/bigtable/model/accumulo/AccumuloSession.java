package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.*;
import com.altamiracorp.bigtable.model.exceptions.MutationsWriteException;
import com.altamiracorp.bigtable.model.exceptions.TableDoesNotExistException;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class AccumuloSession extends ModelSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSession.class);

    private static final String ACCUMULO_INSTANCE_NAME = "bigtable.accumulo.instanceName";
    private static final String ACCUMULO_USER = "bigtable.accumulo.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.accumulo.password";
    private static final String ZK_SERVER_NAMES = "bigtable.accumulo.zookeeperServerNames";

    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;

    private Connector connector;
    private BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    private boolean autoflush = true;
    private final Map<String, BatchWriter> batchWriters = new HashMap<String, BatchWriter>();
    private final Set<String> rowDeletingIteratorAttachList = new HashSet<String>();

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
            connector = zk.getConnector(username, new PasswordToken(password));

            Object autoflushObj = properties.get(CONFIG_AUTOFLUSH);
            if (autoflushObj != null) {
                autoflush = Boolean.getBoolean(autoflushObj.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public AccumuloSession() {
        batchWriterConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
        batchWriterConfig.setMaxWriteThreads(10);
        batchWriterConfig.setMaxMemory(1000000L);
    }

    public AccumuloSession(Connector connector, boolean autoflush) {
        this();
        this.connector = connector;
        this.autoflush = autoflush;
    }

    /**
     * @throws MutationsWriteException    Thrown if the Accumulo writer was unable to write mutations
     * @throws TableDoesNotExistException Thrown if an Accumulo writer cannot be setup for the row's table
     */
    @Override
    public void save(Row row, FlushFlag flushFlag) {
        LOGGER.trace("save called with parameters: row=?", row);
        try {
            BatchWriter writer = getBatchWriter(row.getTableName());
            AccumuloHelper.addRowToWriter(writer, row);
            flush(writer, flushFlag);
        } catch (MutationsRejectedException e) {
            throw new MutationsWriteException("Error occured when writing mutation", e);
        }
    }

    private void flush(BatchWriter writer, FlushFlag flushFlag) throws MutationsRejectedException {
        switch (flushFlag) {
            case DEFAULT:
                if (autoflush) {
                    writer.flush();
                }
                break;
            case FLUSH:
                writer.flush();
                break;
            case NO_FLUSH:
                break;
            default:
                throw new RuntimeException("Unexpected flush flag: " + flushFlag);
        }
    }

    /**
     * @throws MutationsWriteException    Thrown if the Accumulo writer was unable to write mutations
     * @throws TableDoesNotExistException Thrown if an Accumulo writer cannot be setup for the row's table
     */
    @Override
    public void saveMany(String tableName, Collection<Row> rows) {
        LOGGER.trace("saveMany called with parameters: tableName=?, rows=?", tableName, rows);
        if (rows.size() == 0) {
            return;
        }
        try {
            BatchWriter writer = getBatchWriter(tableName);
            for (Row row : rows) {
                AccumuloHelper.addRowToWriter(writer, row);
            }
            if (autoflush) {
                writer.flush();
            }
        } catch (MutationsRejectedException e) {
            throw new MutationsWriteException("Error occured while writing batch of mutations", e);
        }
    }

    @Override
    public Iterable<Row> findByRowKeyRange(String tableName, String rowKeyStart, String rowKeyEnd, ModelUserContext user) {
        LOGGER.trace("findByRowKeyRange called with parameters: tableName=?, rowKeyStart=?, rowKeyEnd=?, user=?", tableName, rowKeyStart, rowKeyEnd, user);

        return scanTableRange(tableName, new Range(rowKeyStart, rowKeyEnd), user);
    }

    @Override
    public Iterable<Row> findByRowStartsWith(String tableName, String rowKeyPrefix, ModelUserContext user) {
        LOGGER.trace(String.format("Scanning table (%s) key range with prefix: %s", tableName, rowKeyPrefix));

        return scanTableRange(tableName, Range.prefix(rowKeyPrefix), user);
    }

    private Iterable<Row> scanTableRange(final String tableName, final Range scannerRange, final ModelUserContext user) {
        try {
            final Scanner scanner = createScanner(tableName, user);
            scanner.setRange(scannerRange);

            return AccumuloHelper.scannerToRows(tableName, scanner);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Row> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user) {
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
            return AccumuloHelper.scannerToRows(tableName, scanner);
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
            Iterator<Row> rows = AccumuloHelper.scannerToRows(tableName, scanner).iterator();
            if (!rows.hasNext()) {
                return null;
            }
            Row result = rows.next();
            if (rows.hasNext()) {
                throw new RuntimeException("Too many rows returned for a single row query (rowKey: " + rowKey + ")");
            }
            return result;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Scanner createScanner(String tableName, ModelUserContext user) throws TableNotFoundException {
        ensureRowDeletingIteratorIsAttached(tableName);

        Scanner scanner = connector.createScanner(tableName, ((AccumuloUserContext) user).getAuthorizations());
        IteratorSetting iteratorSetting = new IteratorSetting(
                100,
                RowDeletingIterator.class.getSimpleName(),
                RowDeletingIterator.class
        );
        scanner.addScanIterator(iteratorSetting);
        return scanner;
    }

    private void ensureRowDeletingIteratorIsAttached(String tableName) {
        try {
            if (rowDeletingIteratorAttachList.contains(tableName)) {
                return;
            }

            IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
            if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                connector.tableOperations().attachIterator(tableName, is);
            }
            rowDeletingIteratorAttachList.add(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            Iterator<Row> rows = AccumuloHelper.scannerToRows(tableName, scanner).iterator();
            if (!rows.hasNext()) {
                return null;
            }
            Row result = rows.next();
            if (rows.hasNext()) {
                throw new RuntimeException("Too many rows returned for a single row query (rowKey: " + rowKey + ")");
            }
            return result;
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
            // If two threads happen to call initializeTable at the same time there could be a race condition and
            // it is ok if the table already exists.
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
            BatchWriter writer = connector.createBatchWriter(tableName, batchWriterConfig);
            try {
                Mutation mutation = new Mutation(rowKey.toString());
                mutation.put(new byte[0], new byte[0], RowDeletingIterator.DELETE_ROW_VALUE.get());
                writer.addMutation(mutation);
                if (autoflush) {
                    writer.flush();
                }
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
            BatchWriter writer = getBatchWriter(tableName);
            Mutation mutation = createMutationFromRow(row);
            mutation.putDelete(new Text(columnFamily), new Text(columnQualifier));
            writer.addMutation(mutation);
            if (autoflush) {
                writer.flush();
                connector.tableOperations().flush(tableName, null, null, true);
            }
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
        return new ArrayList<String>(connector.tableOperations().list());
    }

    /**
     * @throws MutationsWriteException Thrown if the Accumulo writer was unable to write mutations while closing
     */
    @Override
    public void close() {
        flush();
        ArrayList<Map.Entry<String, BatchWriter>> localBatchWriters = createCopyOfBatchWriters();
        for (Map.Entry<String, BatchWriter> writer : localBatchWriters) {
            try {
                writer.getValue().close();
            } catch (MutationsRejectedException e) {
                throw new MutationsWriteException("Could not close writer for table: " + writer.getKey(), e);
            }
        }
    }

    private ArrayList<Map.Entry<String, BatchWriter>> createCopyOfBatchWriters() {
        ArrayList<Map.Entry<String, BatchWriter>> localBatchWriters;
        synchronized (batchWriters) {
            localBatchWriters = new ArrayList<Map.Entry<String, BatchWriter>>(batchWriters.entrySet());
        }
        return localBatchWriters;
    }

    /**
     * @throws MutationsWriteException Thrown if the Accumulo writer was unable to write mutations while flushing
     */
    @Override
    public void flush() {
        ArrayList<Map.Entry<String, BatchWriter>> localBatchWriters = createCopyOfBatchWriters();
        for (Map.Entry<String, BatchWriter> writer : localBatchWriters) {
            try {
                writer.getValue().flush();
            } catch (MutationsRejectedException e) {
                throw new MutationsWriteException("Could not flush writer for table: " + writer.getKey(), e);
            }
        }
    }

    @Override
    public ModelUserContext createModelUserContext(String... authorizations) {
        if (authorizations.length == 1 && authorizations[0].length() == 0) {
            return new AccumuloUserContext(new Authorizations());
        }
        return new AccumuloUserContext(new Authorizations(authorizations));
    }

    private BatchWriter getBatchWriter(String tableName) {
        try {
            synchronized (batchWriters) {
                BatchWriter writer = batchWriters.get(tableName);
                if (writer == null) {
                    writer = connector.createBatchWriter(tableName, batchWriterConfig);
                    batchWriters.put(tableName, writer);
                }
                return writer;
            }
        } catch (TableNotFoundException e) {
            throw new TableDoesNotExistException("Could not find table: " + tableName, e);
        }
    }

    public Connector getConnector() {
        return connector;
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

    @Override
    public void alterAllColumnsVisibility(Row row, String visibility, ModelUserContext user, FlushFlag flushFlag) {
        String tableName = row.getTableName();
        Row copyRow = new Row(tableName, row.getRowKey());
        Collection<ColumnFamily> columnFamilies = row.getColumnFamilies();
        for (ColumnFamily columnFamily : columnFamilies) {
            ColumnFamily copyColumnFamily = new ColumnFamily(columnFamily.getColumnFamilyName());
            for (Column column : columnFamily.getColumns()) {
                Column copyColumn = new Column(column.getName(), column.getValue(), visibility);
                copyColumnFamily.addColumn(copyColumn);
            }
            copyRow.addColumnFamily(copyColumnFamily);
        }
        deleteRow(tableName, row.getRowKey(), user);
        save(copyRow, flushFlag);
    }

}
