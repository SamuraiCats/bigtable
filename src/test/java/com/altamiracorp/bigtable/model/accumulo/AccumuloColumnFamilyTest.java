package com.altamiracorp.bigtable.model.accumulo;

import com.altamiracorp.bigtable.model.ColumnFamily;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.RowKey;
import com.altamiracorp.bigtable.model.Value;
import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mock.MockConnector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AccumuloColumnFamilyTest {

    private static final String TEST_TABLE_NAME = "testTable";
    private AccumuloSession accumuloSession;
    private MockInstance mockInstance;
    private MockConnector connector;

    private AccumuloUserContext adminUser;
    private AccumuloUserContext queryUser;

    @Before
    public void before() throws AccumuloSecurityException, AccumuloException {
        MockitoAnnotations.initMocks(this);

        mockInstance = new MockInstance();
        AuthInfo authInfo = new AuthInfo();
        authInfo.setUser("testUser");
        authInfo.setPassword("testPassword".getBytes());
        connector = (MockConnector) mockInstance.getConnector(authInfo);

        adminUser = new AccumuloUserContext(new Authorizations("A", "B"));
        queryUser = new AccumuloUserContext(new Authorizations("B"));

        accumuloSession = new AccumuloSession(connector);
        accumuloSession.initializeTable(TEST_TABLE_NAME, adminUser);
    }

    @Test
    public void testSetWithColumnVisibility() {
        Row row = new Row<RowKey>(TEST_TABLE_NAME, new RowKey("testRowKey1"));
        AccumuloColumnFamily columnFamily = new AccumuloColumnFamily("testColumnFamily1");
        columnFamily.set("testColumn1", "testValue1");
        columnFamily.set("testColumn2", new Value("testValue2"), new ColumnVisibility("A|B"));
        columnFamily.set("testColumn3", new Value("testValue3"), new ColumnVisibility("A"));
        row.addColumnFamily(columnFamily);

        accumuloSession.save(row, adminUser);

        Row adminQueryRow = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", adminUser);
        ColumnFamily adminQueryColumnFamily = adminQueryRow.get("testColumnFamily1");
        assertEquals(3, adminQueryColumnFamily.getColumns().size());

        Row staffQueryRow = accumuloSession.findByRowKey(TEST_TABLE_NAME, "testRowKey1", queryUser);
        ColumnFamily staffQueryColumnFamily = staffQueryRow.get("testColumnFamily1");
        assertEquals(2, staffQueryColumnFamily.getColumns().size());
    }
}
