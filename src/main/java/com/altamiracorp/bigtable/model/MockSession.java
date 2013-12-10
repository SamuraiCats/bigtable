package com.altamiracorp.bigtable.model;


import com.altamiracorp.bigtable.model.user.ModelUserContext;

import java.util.*;
import java.util.regex.Pattern;

public class MockSession extends ModelSession {
    public HashMap<String, List<Row<? extends RowKey>>> tables = new HashMap<String, List<Row<? extends RowKey>>>();

    @Override
    public void init(Map<String, String> properties) {
    }

    @Override
    public void save(Row<? extends RowKey> row, ModelUserContext user) {
        List<Row<? extends RowKey>> table = tables.get(row.getTableName());
        if (table == null) {
            throw new NullPointerException("Could not find table with name: " + row.getTableName());
        }
        table.add(row);
    }

    @Override
    public void saveMany(String tableName, Collection<Row<? extends RowKey>> rows, ModelUserContext user) {
        for (Row<? extends RowKey> r : rows) {
            save(r, user);
        }
    }

    @Override
    public List<Row<? extends RowKey>> findByRowKeyRange(String tableName, String keyStart, String keyEnd, ModelUserContext user) {
        List<Row<? extends RowKey>> rows = this.tables.get(tableName);
        ArrayList<Row<? extends RowKey>> results = new ArrayList<Row<? extends RowKey>>();
        for (Row<? extends RowKey> row : rows) {
            String rowKey = row.getRowKey().toString();
            if (rowKey.compareTo(keyStart) >= 0 && rowKey.compareTo(keyEnd) < 0) {
                results.add(row);
            }
        }
        Collections.sort(results, new RowKeyComparator());
        return results;
    }

    @Override
    public List<Row<? extends RowKey>> findByRowStartsWith(String tableName, String rowKeyPrefix, ModelUserContext user) {
        List<Row<? extends RowKey>> rows = this.tables.get(tableName);
        ArrayList<Row<? extends RowKey>> results = new ArrayList<Row<? extends RowKey>>();
        for (Row<? extends RowKey> row : rows) {
            String rowKey = row.getRowKey().toString();
            if (rowKey.startsWith(rowKeyPrefix)) {
                results.add(row);
            }
        }
        Collections.sort(results, new RowKeyComparator());
        return results;
    }

    @Override
    public List<Row<? extends RowKey>> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user) {
        List<Row<? extends RowKey>> rows = this.tables.get(tableName);
        if (rows == null) {
            throw new RuntimeException("Unable to find table " + tableName + ". Did you remember to call initializeTable() in Session.initialieTables()?");
        }

        List<Row<? extends RowKey>> result = new ArrayList<Row<? extends RowKey>>();
        for (Row<? extends RowKey> row : rows) {
            if (!Pattern.matches(rowKeyRegex, row.getRowKey().toString())) {
                result.add(row);
            }
        }
        return result;
    }

    @Override
    public Row<? extends RowKey> findByRowKey(String tableName, String rowKey, ModelUserContext user) {
        List<Row<? extends RowKey>> rows = this.tables.get(tableName);
        if (rows == null) {
            throw new RuntimeException("Unable to find table " + tableName + ". Did you remember to call initializeTable() in Session.initialieTables()?");
        }
        for (Row<? extends RowKey> row : rows) {
            if (row.getRowKey().toString().equals(rowKey)) {
                return row;
            }
        }
        return null;
    }

    @Override
    public Row<? extends RowKey> findByRowKey(String tableName, String rowKey, Map<String, String> columnsToReturn, ModelUserContext user) {
        return findByRowKey(tableName, rowKey, user);
    }

    @Override
    public void initializeTable(String tableName, ModelUserContext user) {
        this.tables.put(tableName, new ArrayList<Row<? extends RowKey>>());
    }

    @Override
    public void deleteTable(String tableName, ModelUserContext user) {
        this.tables.remove(tableName);
    }

    @Override
    public void deleteRow(String tableName, RowKey rowKey, ModelUserContext user) {
        String rowKeyStr = rowKey.toString();
        List<Row<? extends RowKey>> rows = this.tables.get(tableName);
        for (int i = 0; i < rows.size(); i++) {
            if (rowKeyStr.equals(rows.get(i).getRowKey().toString())) {
                rows.remove(i);
                return;
            }
        }
    }

    public void deleteColumn(Row<? extends RowKey> row, String tableName, String columnFamily, String columnQualifier, ModelUserContext user) {
        List<ColumnFamily> columnFamilies = (List<ColumnFamily>) row.getColumnFamilies();
        for (int i = 0; i < columnFamilies.size(); i++) {
            if (columnFamilies.get(i).getColumnFamilyName().equals(columnFamily)) {
                List<Column> columns = (List<Column>) columnFamilies.get(i).getColumns();
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.get(j).getName().equals(columnQualifier)) {
                        columns.remove(j);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public List<String> getTableList(ModelUserContext user) {
        return new ArrayList<String>(this.tables.keySet());
    }

    @Override
    public void close() {
    }
}
