package com.altamiracorp.bigtable.model;


import com.altamiracorp.bigtable.model.user.ModelUserContext;

import java.util.*;
import java.util.regex.Pattern;

public class MockSession extends ModelSession {
    public HashMap<String, List<Row>> tables = new HashMap<String, List<Row>>();

    @Override
    public void init(Map properties) {
    }

    @Override
    public void save(Row row, ModelUserContext user) {
        List<Row> table = tables.get(row.getTableName());
        if (table == null) {
            throw new NullPointerException("Could not find table with name: " + row.getTableName());
        }
        table.add(row);
    }

    @Override
    public void saveMany(String tableName, Collection<Row> rows, ModelUserContext user) {
        for (Row r : rows) {
            save(r, user);
        }
    }

    @Override
    public List<Row> findByRowKeyRange(String tableName, String keyStart, String keyEnd, ModelUserContext user) {
        List<Row> rows = this.tables.get(tableName);
        ArrayList<Row> results = new ArrayList<Row>();
        for (Row row : rows) {
            String rowKey = row.getRowKey().toString();
            if (rowKey.compareTo(keyStart) >= 0 && rowKey.compareTo(keyEnd) < 0) {
                results.add(row);
            }
        }
        Collections.sort(results, new RowKeyComparator());
        return results;
    }

    @Override
    public List<Row> findByRowStartsWith(String tableName, String rowKeyPrefix, ModelUserContext user) {
        List<Row> rows = this.tables.get(tableName);
        ArrayList<Row> results = new ArrayList<Row>();
        for (Row row : rows) {
            String rowKey = row.getRowKey().toString();
            if (rowKey.startsWith(rowKeyPrefix)) {
                results.add(row);
            }
        }
        Collections.sort(results, new RowKeyComparator());
        return results;
    }

    @Override
    public List<Row> findByRowKeyRegex(String tableName, String rowKeyRegex, ModelUserContext user) {
        List<Row> rows = this.tables.get(tableName);
        if (rows == null) {
            throw new RuntimeException("Unable to find table " + tableName + ". Did you remember to call initializeTable() in Session.initialieTables()?");
        }

        List<Row> result = new ArrayList<Row>();
        for (Row row : rows) {
            if (!Pattern.matches(rowKeyRegex, row.getRowKey().toString())) {
                result.add(row);
            }
        }
        return result;
    }

    @Override
    public Row findByRowKey(String tableName, String rowKey, ModelUserContext user) {
        List<Row> rows = this.tables.get(tableName);
        if (rows == null) {
            throw new RuntimeException("Unable to find table " + tableName + ". Did you remember to call initializeTable() in Session.initialieTables()?");
        }
        for (Row row : rows) {
            if (row.getRowKey().toString().equals(rowKey)) {
                return row;
            }
        }
        return null;
    }

    @Override
    public Row findByRowKey(String tableName, String rowKey, Map<String, String> columnsToReturn, ModelUserContext user) {
        return findByRowKey(tableName, rowKey, user);
    }

    @Override
    public void initializeTable(String tableName, ModelUserContext user) {
        this.tables.put(tableName, new ArrayList<Row>());
    }

    @Override
    public void deleteTable(String tableName, ModelUserContext user) {
        this.tables.remove(tableName);
    }

    @Override
    public void deleteRow(String tableName, RowKey rowKey, ModelUserContext user) {
        String rowKeyStr = rowKey.toString();
        List<Row> rows = this.tables.get(tableName);
        for (int i = 0; i < rows.size(); i++) {
            if (rowKeyStr.equals(rows.get(i).getRowKey().toString())) {
                rows.remove(i);
                return;
            }
        }
    }

    public void deleteColumn(Row row, String tableName, String columnFamily, String columnQualifier, ModelUserContext user) {
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
