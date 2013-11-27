package com.altamiracorp.bigtable.model;

import java.util.Comparator;

public class RowKeyComparator implements Comparator<Row<? extends RowKey>> {
    @Override
    public int compare(Row<? extends RowKey> row1, Row<? extends RowKey> row2) {
        return row1.getRowKey().toString().compareTo(row2.getRowKey().toString());
    }
}
