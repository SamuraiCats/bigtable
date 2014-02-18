package com.altamiracorp.bigtable.model;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Collection;
import java.util.HashMap;

public class ColumnFamily {
    private final String columnFamilyName;
    private final HashMap<String, Column> columns = new HashMap<String, Column>();

    public ColumnFamily(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public void addColumn(Column column) {
        this.columns.put(column.getName(), column);
        column.setDirty(true);
    }

    public ColumnFamily addColumns(Collection<Column> columns) {
        for (Column column : columns) {
            addColumn(column);
        }
        return this;
    }

    public Value get(String columnName) {
        Column column = this.columns.get(columnName);
        if (column == null) {
            return null;
        }
        return column.getValue();
    }

    public ColumnFamily set(String columnName, Object value) {
        addColumn(new Column(columnName, value));
        return this;
    }

    public ColumnFamily set (String columnName, Object value, String visibility) {
        addColumn(new Column(columnName, value, visibility));
        return this;
    }

    public String getColumnFamilyName() {
        return this.columnFamilyName;
    }

    public Collection<Column> getColumns() {
        return this.columns.values();
    }

    public JSONObject toJson() {
        try {
            JSONObject json = new JSONObject();
            for (Column column : getColumns()) {
                json.put(column.getName(), column.getValue());
            }
            return json;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        toString(result, "");
        return result.toString();
    }

    public void toString(StringBuilder out, String indent) {
        out.append(indent + getColumnFamilyName() + "\n");
        for (Column column : getColumns()) {
            column.toString(out, indent + "\t");
        }
    }

    public void setDirtyBit(boolean val) {
        for (Column column : getColumns()) {
            column.setDirty(val);
        }
    }

    public void update(ColumnFamily newColumnFamily) {
        for (Column newColumn : newColumnFamily.getColumns()) {
            addColumn(newColumn);
        }
    }
}
