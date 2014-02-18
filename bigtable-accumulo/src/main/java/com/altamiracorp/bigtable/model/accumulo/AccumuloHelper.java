package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.Column;
import com.altamiracorp.bigtable.model.ColumnFamily;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.RowKey;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;

import java.util.*;

public class AccumuloHelper {
    public static boolean addRowToWriter(BatchWriter writer, Row row) throws MutationsRejectedException {
        if (row == null) {
            throw new NullPointerException("row cannot be null");
        }
        if (row.getRowKey() == null) {
            throw new NullPointerException("rowKey cannot be null");
        }
        String rowKey = row.getRowKey().toString();
        if (rowKey.length() == 0) {
            throw new NullPointerException("rowKey cannot have 0 length");
        }
        Mutation mutation = new Mutation(rowKey);
        Collection<ColumnFamily> columnFamilies = row.getColumnFamilies();
        for (ColumnFamily columnFamily : columnFamilies) {
            addColumnFamilyToMutation(mutation, columnFamily);
        }
        if (mutation.size() == 0) {
            return false;
        }
        writer.addMutation(mutation);
        return true;
    }

    private static void addColumnFamilyToMutation(Mutation mutation, ColumnFamily columnFamily) {
        for (Column column : columnFamily.getColumns()) {
            if (column.isDelete()) {
                addColumnDeleteToMutation(mutation, column, columnFamily.getColumnFamilyName());
            } else if (column.isDirty()) {
                addColumnToMutation(mutation, column, columnFamily.getColumnFamilyName());
            }
        }
    }

    private static void addColumnDeleteToMutation(Mutation mutation, Column column, String columnFamilyName) {
        if (column.getVisibility() != null) {
            mutation.putDelete(columnFamilyName, column.getName(), new ColumnVisibility(column.getVisibility()));
        } else {
            mutation.putDelete(columnFamilyName, column.getName());
        }
    }

    private static void addColumnToMutation(Mutation mutation, Column column, String columnFamilyName) {
        com.altamiracorp.bigtable.model.Value v = column.getValue();
        Value value = null;
        if (v != null) {
            value = new Value(v.toBytes());
        }

        if (column.getVisibility() != null) {
            mutation.put(columnFamilyName, column.getName(), new ColumnVisibility(column.getVisibility()), value);
        } else {
            mutation.put(columnFamilyName, column.getName(), value);
        }
    }

    public static Iterable<Row> scannerToRows(final String tableName, final ScannerBase scanner) {
        return new Iterable<Row>() {
            @Override
            public Iterator<Row> iterator() {
                final RowIterator rowIterator = new RowIterator(scanner);
                return new Iterator<Row>() {
                    @Override
                    public boolean hasNext() {
                        return rowIterator.hasNext();
                    }

                    @Override
                    public Row next() {
                        Iterator<Map.Entry<Key, Value>> row = rowIterator.next();
                        return accumuloRowToRow(tableName, row);
                    }

                    @Override
                    public void remove() {
                        rowIterator.remove();
                    }
                };
            }
        };
    }

    public static List<ColumnFamily> scannerToColumnFamiliesFilteredByRegex(Scanner scanner,
                                                                            long colFamOffset, long colFamLimit, String colFamRegex) {
        List<ColumnFamily> colFams = new ArrayList<ColumnFamily>();
        String rowKey = scanner.getRange().getStartKey().getRow().toString();

        scanner.setBatchSize(100);
        IteratorSetting iter = new IteratorSetting(15, "regExFilter", RegExFilter.class);
        RegExFilter.setRegexs(iter, null, colFamRegex, null, null, false);
        scanner.addScanIterator(iter);

        long count = 0;
        PeekingIterator<Map.Entry<Key, Value>> iterator = new PeekingIterator<Map.Entry<Key, Value>>(scanner.iterator());

        while (iterator.hasNext() && count < colFamOffset + colFamLimit &&
                iterator.peek().getKey().getRow().toString().equals(rowKey)) {
                ColumnFamily colFam = getNextColumnFamily(iterator);

            if (count >= colFamOffset) {
                colFams.add(colFam);
            }

            count++;
        }

        return colFams;
    }

    public static ColumnFamily getNextColumnFamily(PeekingIterator<Map.Entry<Key, Value>> iterator) {
        String colFamName = iterator.peek().getKey().getColumnFamily().toString();
        ColumnFamily colFam = new ColumnFamily(colFamName);

        while (iterator.peek() != null && iterator.peek().getKey().getColumnFamily().toString().equals(colFamName)) {
            System.out.println(iterator.peek());
            Map.Entry<Key, Value> next = iterator.next();
            colFam.addColumn(new Column(next.getKey().getColumnQualifier().toString(), next.getValue().toString(), next.getKey().getColumnVisibility().toString()));
        }

        return colFam;
    }

    public static Row accumuloRowToRow(String tableName, Iterator<Map.Entry<Key, Value>> accumuloRow) {
        Row<RowKey> row = null;
        while (accumuloRow.hasNext()) {
            Map.Entry<Key, Value> accumuloColumn = accumuloRow.next();
            if (row == null) {
                String rowKey = accumuloColumn.getKey().getRow().toString();
                row = new Row<RowKey>(tableName, new RowKey(rowKey));
            }
            String columnFamilyString = accumuloColumn.getKey().getColumnFamily().toString();
            ColumnFamily columnFamily = row.get(columnFamilyString);
            if (columnFamily == null) {
                row.addColumnFamily(new ColumnFamily(columnFamilyString));
                columnFamily = row.get(columnFamilyString);
            }

            String columnNameString = accumuloColumn.getKey().getColumnQualifier().toString();
            columnFamily.set(columnNameString, accumuloValueToObject(accumuloColumn.getValue()), accumuloColumn.getKey().getColumnVisibility().toString());
        }
        row.setDirtyBits(false);
        return row;
    }

    private static byte[] accumuloValueToObject(Value value) {
        return value.get();
    }


}
