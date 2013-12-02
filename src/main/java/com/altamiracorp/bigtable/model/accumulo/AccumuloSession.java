package com.altamiracorp.bigtable.model.accumulo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.altamiracorp.bigtable.model.Column;
import com.altamiracorp.bigtable.model.ColumnFamily;
import com.altamiracorp.bigtable.model.ModelSession;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.RowKey;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;

public class AccumuloSession extends ModelSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSession.class);

    private static final String ACCUMULO_INSTANCE_NAME = "bigtable.accumulo.instanceName";
    private static final String ACCUMULO_USER = "bigtable.accumulo.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.accumulo.password";
    private static final String ZK_SERVER_NAMES = "bigtable.accumulo.zookeeperServerNames";

    private static final String ROW_DELETING_ITERATOR_NAME = "RowDeletingIterator";
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    		
    private Connector connector;
    private long maxMemory = 1000000L;
    private long maxLatency = 1000L;
    private int maxWriteThreads = 10;

    @Override
    public void init(Map<String, String> properties) {
    	LOGGER.trace("init called with parameters: properties=?", properties);
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
    public void saveMany(String tableName, Collection<Row<? extends RowKey>> rows, ModelUserContext user) {
    	LOGGER.trace("saveMany called with parameters: tableName=?, rows=?, user=?", tableName, rows, user);
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
    	LOGGER.trace("findByRowKeyRange called with parameters: tableName=?, rowKeyStart=?, rowKeyEnd=?, user=?", tableName, rowKeyStart, rowKeyEnd, user);
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
    	//Accumulo ranges work by inclusive start/exclusive end principle.  When searching for a specific
    	//record, it is suggested to add a null terminator ('\0'), as this will not be used by other strings
        return findByRowKeyRange(tableName, rowKeyPrefix, rowKeyPrefix + "\0", user);
    }

    @Override
    public List<Row<? extends RowKey>> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user) {
    	LOGGER.trace("findByRowKeyRegex called with parameters: tableName=?, rowKeyRegex=?, user=?", tableName, rowKeyRegex, user);
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
    	LOGGER.trace("findByRowKey called with parameters: tableName=?, rowKey=?, user=?", tableName, rowKey, user);
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
    	LOGGER.trace("findByRowKey called with parameters: tableName=?, rowKey=?, columnsToReturn=?, user=?", tableName, rowKey, columnsToReturn, user);
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
		LOGGER.trace(
				"deleteRow called with parameters: tableName=?, rowKey=?, user=?",
				tableName, rowKey, user);
		// In most instances (e.g., when reading is not necessary), the
		// RowDeletingIterator gives better performance than the deleting
		// mutation. This is due to the fact that Deleting mutations marks each
		// entry with a delete marker. Using the iterator marks a whole row with
		// a single mutation.
		try {
			BatchWriter writer = connector.createBatchWriter(tableName,
					getMaxMemory(), getMaxLatency(), getMaxWriteThreads());
			try {
				IteratorSetting is = new IteratorSetting(
						ROW_DELETING_ITERATOR_PRIORITY,
						ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
				connector.tableOperations().attachIterator(tableName, is);
				Mutation mutation = new Mutation(rowKey.toString());
				mutation.put("", "", RowDeletingIterator.DELETE_ROW_VALUE);
				writer.flush();
				connector.tableOperations().flush(tableName, null, null, true);
			} catch (AccumuloException ae) {
				throw new RuntimeException(ae);
			} catch (AccumuloSecurityException ase) {
				throw new RuntimeException(ase);
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
    public void deleteColumn(Row<? extends RowKey> row, String tableName, String columnFamily, String columnQualifier, ModelUserContext user) {
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

    public static Mutation createMutationFromRow(Row<? extends RowKey> row) {
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
