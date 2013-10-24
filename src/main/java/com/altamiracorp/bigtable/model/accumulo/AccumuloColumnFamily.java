package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.ColumnFamily;
import org.apache.accumulo.core.security.ColumnVisibility;

public class AccumuloColumnFamily extends ColumnFamily {

    /**
     * Decorator
     * @param columnFamily
     */
    public AccumuloColumnFamily (ColumnFamily columnFamily) {
        super(columnFamily.getColumnFamilyName());
        this.addColumns(columnFamily.getColumns());
    }

    public AccumuloColumnFamily (String columnFamilyName) {
        super(columnFamilyName);
    }

    public ColumnFamily set(String columnName, Object value, ColumnVisibility columnVisibility) {
        addColumn(new AccumuloColumn(columnName, value, columnVisibility));
        return this;
    }
}
