package com.altamiracorp.bigtable.model;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Collection;
import java.util.HashMap;

public class Row<TRowKey extends RowKey> {
    private TRowKey rowKey;
    private HashMap<String, ColumnFamily> columnFamilies = new HashMap<String, ColumnFamily>();
    private String tableName;

    public Row(String tableName, TRowKey rowKey) {
        this(tableName);
        this.rowKey = rowKey;
    }

    public Row(String tableName) {
        this.tableName = tableName;
    }

    public TRowKey getRowKey() {
        return rowKey;
    }

    public void addColumnFamily(ColumnFamily columnFamily) {
        this.columnFamilies.put(columnFamily.getColumnFamilyName(), columnFamily);
    }

    @SuppressWarnings("unchecked")
	public <T extends ColumnFamily> T get(String columnFamilyName) {
        return (T) this.columnFamilies.get(columnFamilyName);
    }

    public Collection<ColumnFamily> getColumnFamilies() {
        return this.columnFamilies.values();
    }

    public String getTableName() {
        return this.tableName;
    }

    public JSONObject toJson() {
        try {
            JSONObject json = new JSONObject();
            json.put("key", getRowKey().toJson());
            json.put("tableName", getTableName());
            for (ColumnFamily columnFamily : getColumnFamilies()) {
                json.put(columnFamily.getColumnFamilyName(), columnFamily.toJson());
            }
            return json;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(getTableName() + ": " + getRowKey() + "\n");
        for (ColumnFamily columnFamily : getColumnFamilies()) {
            columnFamily.toString(result, "\t");
        }
        return result.toString();
    }

    public void setDirtyBits(boolean val) {
        for (ColumnFamily columnFamily : getColumnFamilies()) {
            columnFamily.setDirtyBit(val);
        }
    }

    public void update(Row<TRowKey> newRow) {
        for (ColumnFamily newColumnFamily : newRow.getColumnFamilies()) {
            ColumnFamily existingColumnFamily = get(newColumnFamily.getColumnFamilyName());
            if (existingColumnFamily == null) {
                addColumnFamily(newColumnFamily);
            } else {
                existingColumnFamily.update(newColumnFamily);
            }
        }
    }
}
