package com.altamiracorp.bigtable.model;

import java.util.Comparator;

public class RowKeyComparator implements Comparator<Row> {
    @Override
    public int compare(Row row1, Row row2) {
        return row1.getRowKey().toString().compareTo(row2.getRowKey().toString());
    }
}
