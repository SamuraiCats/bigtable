package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.Column;
import org.apache.accumulo.core.security.ColumnVisibility;

public class AccumuloColumn extends Column{

    private ColumnVisibility columnVisibility;

    public AccumuloColumn (String name, Object value) {
        super(name,value);
    }

    public AccumuloColumn (String name, Object value, ColumnVisibility columnVisibility) {
        super(name,value);
        this.columnVisibility = columnVisibility;
    }

    public ColumnVisibility getColumnVisibility () {
        return columnVisibility;
    }

    public void setColumnVisibility (ColumnVisibility columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

}
