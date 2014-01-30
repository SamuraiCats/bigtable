package com.altamiracorp.bigtable.model;

import com.altamiracorp.bigtable.model.user.ModelUserContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class ModelSession {
    protected static final String CONFIG_AUTOFLUSH = "bigtable.autoflush";

    public abstract void init(Map<String, Object> properties);

    /**
     * Save a row
     *
     * @param row
     * @param user
     */
    public void save(Row row, ModelUserContext user) {
        save(row, FlushFlag.DEFAULT, user);
    }

    /**
     * Save a row
     *
     * @param row
     * @param flushFlag
     * @param user
     */
    public abstract void save(Row row, FlushFlag flushFlag, ModelUserContext user);

    /**
     * Save a collection of rows
     *
     * @param tableName
     * @param rows
     * @param user
     */
    public abstract void saveMany(String tableName, Collection<Row> rows, ModelUserContext user);

    /**
     * Find rows in a range of specified row keys
     *
     * @param tableName
     * @param keyStart
     * @param keyEnd
     * @param user
     * @return
     */
    public abstract Iterable<Row> findByRowKeyRange(String tableName, String keyStart, String keyEnd, ModelUserContext user);

    /**
     * Find rows based on the prefix of the row keys
     *
     * @param tableName
     * @param rowKeyPrefix
     * @param user
     * @return
     */
    public abstract Iterable<Row> findByRowStartsWith(String tableName, String rowKeyPrefix, ModelUserContext user);

    /**
     * Find rows with the group of row keys that match the provided regular expression
     *
     * @param tableName
     * @param rowKeyRegex
     * @param user
     * @return
     */
    public abstract Iterable<Row> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user);

    public abstract Iterable<Row> findAll(String tableName, ModelUserContext user);

    public abstract long rowCount(String tableName, ModelUserContext user);

    /**
     * Returns an entire row with the specified row key
     *
     * @param tableName
     * @param rowKey
     * @param user
     * @return
     */
    public abstract Row findByRowKey(String tableName, String rowKey, ModelUserContext user);

    /**
     * Returns a row, with only the columns specified, with the specified row key
     *
     * @param tableName
     * @param rowKey
     * @param columnsToReturn
     * @param user
     * @return
     */
    public abstract Row findByRowKey(String tableName, String rowKey, Map<String, String> columnsToReturn, ModelUserContext user);

    /**
     * Initialize a table
     *
     * @param tableName
     * @param user
     */
    public abstract void initializeTable(String tableName, ModelUserContext user);

    /**
     * Delete a table
     *
     * @param tableName
     * @param user
     */
    public abstract void deleteTable(String tableName, ModelUserContext user);

    /**
     * Delete a row with the specified row key
     *
     * @param tableName
     * @param rowKey
     * @param user
     */
    public abstract void deleteRow(String tableName, RowKey rowKey, ModelUserContext user);

    /**
     * Delete a specific column on the provided row
     *
     * @param row
     * @param tableName
     * @param columnFamily
     * @param columnQualifier
     * @param user
     */
    public abstract void deleteColumn(Row row, String tableName, String columnFamily, String columnQualifier, ModelUserContext user);

    /**
     * Returns the full list of tables in the provider data store
     *
     * @param user current user
     * @return full list of tables
     */
    public abstract List<String> getTableList(ModelUserContext user);

    /**
     * Close this session, cleanup resources
     */
    public abstract void close();

    /**
     * Flushes any in-queue rows to storage. This method has no effect if autocommit is true.
     */
    public abstract void flush();

    /**
     * Creates ModelUserContext from a list of authorizations.
     *
     * @param authorizations list of authorizations
     * @return The new module user context
     */
    public abstract ModelUserContext createModelUserContext(String... authorizations);
}
