package com.altamiracorp.bigtable.model.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.altamiracorp.bigtable.model.*;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockConnector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;


@RunWith(JUnit4.class)
public class AccumuloSessionTest {
    private static final String TEST_TABLE_NAME = "testTable";
    private AccumuloSession accumuloSession;
    private MockInstance mockInstance;
    private MockConnector connector;
    private Authorizations authorizations;
    private long maxMemory = 1000000L;
    private long maxLatency = 1000L;
    private int maxWriteThreads = 10;
    private AccumuloUserContext queryUser;
    private AccumuloUserContext adminUser;

    @Before
    public void before() throws AccumuloSecurityException, AccumuloException {
        MockitoAnnotations.initMocks(this);

        mockInstance = new MockInstance();
        connector = (MockConnector) mockInstance.getConnector("testUser", new PasswordToken("testPassword".getBytes()));

        authorizations = new Authorizations("ALL");
        queryUser = new AccumuloUserContext(authorizations);
        adminUser = new AccumuloUserContext(new Authorizations("A", "B"));

        accumuloSession = new AccumuloSession(connector, true);
        accumuloSession.initializeTable(TEST_TABLE_NAME, queryUser);
    }

    @Test
    public void testSave() throws TableNotFoundException {
        Row row = new Row<RowKey>(TEST_TABLE_NAME, new RowKey("testRowKey1"));

        ColumnFamily columnFamily1 = new ColumnFamily("testColumnFamily1");
        columnFamily1.set("1testColumn1", "1testColumn1Value");
        columnFamily1.set("1testColumn2", "1testColumn2Value");
        columnFamily1.set("1testColumn3", 111L);
        columnFamily1.set("1testColumn4", "test".getBytes());
        row.addColumnFamily(columnFamily1);

        ColumnFamily columnFamily2 = new ColumnFamily("testColumnFamily2");
        columnFamily2.set("2testColumn1", "2testColumn1Value");
        columnFamily2.set("2testColumn2", "2testColumn2Value");
        columnFamily2.set("2testColumn3", 222L);
        row.addColumnFamily(columnFamily2);

        accumuloSession.save(row, queryUser);

        Scanner scanner = connector.createScanner(TEST_TABLE_NAME, authorizations);
        scanner.setRange(new Range("testRowKey1"));
        RowIterator rowIterator = new RowIterator(scanner);
        int rowCount = 0;
        while (rowIterator.hasNext()) {
            if (rowCount != 0) {
                fail("Too many rows");
            }

            Iterator<Map.Entry<Key, org.apache.accumulo.core.data.Value>> accumuloRow = rowIterator.next();
            int colunnCount = 0;
            while (accumuloRow.hasNext()) {
                Map.Entry<Key, org.apache.accumulo.core.data.Value> accumuloColumn = accumuloRow.next();
                String columnFamilyString = accumuloColumn.getKey().getColumnFamily().toString();
                String columnNameString = accumuloColumn.getKey().getColumnQualifier().toString();

                Assert.assertEquals("testRowKey1", accumuloColumn.getKey().getRow().toString());
                if ("testColumnFamily1".equals(columnFamilyString)) {
                    if ("1testColumn1".equals(columnNameString)) {
                        Assert.assertEquals("1testColumn1Value", accumuloColumn.getValue().toString());
                    } else if ("1testColumn2".equals(columnNameString)) {
                        Assert.assertEquals("1testColumn2Value", accumuloColumn.getValue().toString());
                    } else if ("1testColumn3".equals(columnNameString)) {
                        Assert.assertEquals(111L, new Value(accumuloColumn.getValue().get()).toLong().longValue());
                    } else if ("1testColumn4".equals(columnNameString)) {
                        Assert.assertEquals("test", accumuloColumn.getValue().toString());
                    } else {
                        fail("invalid column name: " + columnFamilyString + " - " + columnNameString);
                    }
                } else if ("testColumnFamily2".equals(columnFamilyString)) {
                    if ("2testColumn1".equals(columnNameString)) {
                        Assert.assertEquals("2testColumn1Value", accumuloColumn.getValue().toString());
                    } else if ("2testColumn2".equals(columnNameString)) {
                        Assert.assertEquals("2testColumn2Value", accumuloColumn.getValue().toString());
                    } else if ("2testColumn3".equals(columnNameString)) {
                        Assert.assertEquals(222L, new Value(accumuloColumn.getValue().get()).toLong().longValue());
                    } else {
                        fail("invalid column name: " + columnFamilyString + " - " + columnNameString);
                    }
                } else {
                    fail("invalid column family name: " + columnFamilyString);
                }
                colunnCount++;
            }
            Assert.assertEquals(7, colunnCount);

            rowCount++;
        }
    }

    @Test
    public void testFindByRowKey() throws TableNotFoundException, MutationsRejectedException {
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE_NAME, maxMemory, maxLatency, maxWriteThreads);
        Mutation mutation = new Mutation("testRowKey");
        mutation.put("testColumnFamily1", "1testColumn1", "1testValue1");
        mutation.put("testColumnFamily1", "1testColumn2", "1testValue2");
        mutation.put("testColumnFamily1", "1testColumn3", new org.apache.accumulo.core.data.Value(new Value(111L).toBytes()));
        mutation.put("testColumnFamily2", "2testColumn1", "2testValue1");
        writer.addMutation(mutation);
        writer.close();

        Row row = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey", queryUser);
        assertEquals(TEST_TABLE_NAME, row.getTableName());
        assertEquals("testRowKey", row.getRowKey().toString());
        assertEquals(2, row.getColumnFamilies().size());

        ColumnFamily testColumnFamily1 = row.get("testColumnFamily1");
        assertEquals(3, testColumnFamily1.getColumns().size());
        assertEquals("1testValue1", testColumnFamily1.get("1testColumn1").toString());
        assertEquals("1testValue2", testColumnFamily1.get("1testColumn2").toString());
        assertEquals(111L, testColumnFamily1.get("1testColumn3").toLong().longValue());

        ColumnFamily testColumnFamily2 = row.get("testColumnFamily2");
        assertEquals(1, testColumnFamily2.getColumns().size());
        assertEquals("2testValue1", testColumnFamily2.get("2testColumn1").toString());
    }

    @Test
    public void testFindByRowStartsWith() throws TableNotFoundException, MutationsRejectedException {
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE_NAME, maxMemory, maxLatency, maxWriteThreads);
        Mutation mutation = new Mutation("testRowKey1");
        mutation.put("testColumnFamily1", "1testColumn1", "1testValue1");
        writer.addMutation(mutation);

        mutation = new Mutation("testRow2");
        mutation.put("testColumnFamily1", "1testColumn1", "2testValue2");
        writer.addMutation(mutation);

        mutation = new Mutation("testRowKeyzSample");
        mutation.put("testColumnFamily1", "1testColumn1", "3testValue3");
        writer.addMutation(mutation);
        writer.close();

        List<Row> row = toList(accumuloSession.findByRowStartsWith(TEST_TABLE_NAME, "testRowKey", queryUser));
        assertEquals(2, row.size());


        // Ensure that only two of the three row keys are returned
        assertEquals("testRowKey1", row.get(0).getRowKey().toString());
        assertEquals("testRowKeyzSample", row.get(1).getRowKey().toString());
    }

    @Test
    public void testFindAll () throws TableNotFoundException, MutationsRejectedException {
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE_NAME, maxMemory, maxLatency, maxWriteThreads);

        List<Row> rows = toList(accumuloSession.findAll(TEST_TABLE_NAME, queryUser));
        assertEquals(0, rows.size());

        Mutation mutation = new Mutation("testRowKey1");
        mutation.put("testColumnFamily1", "testColumn1", "testValue1");
        writer.addMutation(mutation);

        mutation = new Mutation("testRowKey2");
        mutation.put("testColumnFamily2", "testColumn2", "testValue2");
        writer.addMutation(mutation);

        rows = toList(accumuloSession.findAll(TEST_TABLE_NAME, queryUser));
        assertEquals(2, rows.size());
    }

    @Test
    public void testFindByRowKeyRange() throws TableNotFoundException, MutationsRejectedException {
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE_NAME, maxMemory, maxLatency, maxWriteThreads);

        Mutation mutation = new Mutation("testRowKey1");
        mutation.put("testColumnFamily1", "testColumn1", "testValue1");
        writer.addMutation(mutation);

        mutation = new Mutation("testRowKey2");
        mutation.put("testColumnFamily2", "testColumn2", "testValue2");
        writer.addMutation(mutation);

        writer.close();

        List<Row> rows = toList(accumuloSession.findByRowKeyRange(TEST_TABLE_NAME, "testRowKey", "testRowKeyZ", queryUser));
        assertEquals(2, rows.size());

        Row row1 = rows.get(0);
        assertEquals("testRowKey1", row1.getRowKey().toString());
        assertEquals("testValue1", row1.get("testColumnFamily1").get("testColumn1").toString());

        Row row2 = rows.get(1);
        assertEquals("testRowKey2", row2.getRowKey().toString());
        assertEquals("testValue2", row2.get("testColumnFamily2").get("testColumn2").toString());
    }

    @Test
    public void testFindByRowKeyRegex() throws TableNotFoundException, MutationsRejectedException {
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE_NAME, maxMemory, maxLatency, maxWriteThreads);

        Mutation mutation = new Mutation("testRowKey1");
        mutation.put("testColumnFamily1", "testColumn1", "testValue1");
        writer.addMutation(mutation);

        mutation = new Mutation("testRowKey2");
        mutation.put("testColumnFamily2", "testColumn2", "testValue2");
        writer.addMutation(mutation);

        writer.close();

        List<Row> rows = toList(accumuloSession.findByRowKeyRegex(TEST_TABLE_NAME, ".*1", queryUser));
        assertEquals(1, rows.size());

        Row row1 = rows.get(0);
        assertEquals("testRowKey1", row1.getRowKey().toString());
        assertEquals("testValue1", row1.get("testColumnFamily1").get("testColumn1").toString());
    }

    private List<Row> toList(Iterable<Row> rows) {
        List<Row> result = new ArrayList<Row>();
        for (Row row : rows) {
            result.add(row);
        }
        return result;
    }

    @Test
    public void testDeleteRow() throws TableNotFoundException {
        Row row = new Row<RowKey>(TEST_TABLE_NAME, new RowKey("testRowKey1"));
        ColumnFamily columnFamily1 = new ColumnFamily("testColumnFamily1");
        columnFamily1.set("1testColumn1", "1testColumn1Value");
        row.addColumnFamily(columnFamily1);

        accumuloSession.save(row, queryUser);

        row = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", queryUser);
        assertNotNull("row should exist", row);

        accumuloSession.deleteRow(TEST_TABLE_NAME, new RowKey("testRowKey1"), queryUser);

        row = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", queryUser);
        assertNull("row should be deleted", row);
    }

    @Test
    public void testColumnVisibility() {
        AccumuloUserContext queryUserWithAuth = new AccumuloUserContext(new Authorizations("B"));
        Row row = new Row<RowKey>(TEST_TABLE_NAME, new RowKey("testRowKey1"));
        ColumnFamily columnFamily = new ColumnFamily("testColumnFamily1");
        columnFamily.set("testColumn1", "testValue1");
        columnFamily.addColumn(new Column("testColumn2", new Value("testValue2"), "A|B"));
        columnFamily.addColumn(new Column("testColumn3", new Value("testValue3"), "A"));
        row.addColumnFamily(columnFamily);

        accumuloSession.save(row, adminUser);

        Row adminQueryRow = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", adminUser);
        ColumnFamily adminQueryColumnFamily = adminQueryRow.get("testColumnFamily1");
        assertEquals(3, adminQueryColumnFamily.getColumns().size());

        Row staffQueryRow = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", queryUserWithAuth);
        ColumnFamily staffQueryColumnFamily = staffQueryRow.get("testColumnFamily1");
        assertEquals(2, staffQueryColumnFamily.getColumns().size());
    }

    @Test
    public void testSetWithColumnVisibility() {
        Row row = new Row<RowKey>(TEST_TABLE_NAME, new RowKey("testRowKey1"));
        AccumuloUserContext queryUserWithAuth = new AccumuloUserContext(new Authorizations("B"));
        ColumnFamily columnFamily = new ColumnFamily("testColumnFamily1");
        columnFamily.set("testColumn1", "testValue1");
        columnFamily.set("testColumn2", new Value("testValue2"), "A|B");
        columnFamily.set("testColumn3", new Value("testValue3"), "A");
        row.addColumnFamily(columnFamily);

        accumuloSession.save(row, adminUser);

        Row adminQueryRow = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", adminUser);
        ColumnFamily adminQueryColumnFamily = adminQueryRow.get("testColumnFamily1");
        assertEquals(3, adminQueryColumnFamily.getColumns().size());

        Row staffQueryRow = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", queryUserWithAuth);
        ColumnFamily staffQueryColumnFamily = staffQueryRow.get("testColumnFamily1");
        assertEquals(2, staffQueryColumnFamily.getColumns().size());
    }
}
