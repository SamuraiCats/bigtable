package com.altamiracorp.bigtable.model.user.accumulo;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AccumuloUserContextTest {
    @Test
    public void testHashCode() {
        Authorizations authorizations = new Authorizations("a", "b");
        AccumuloUserContext user = new AccumuloUserContext(authorizations);
        assertEquals(authorizations.hashCode(), user.hashCode());
    }

    @Test
    public void testEquals() {
        AccumuloUserContext userAB1 = new AccumuloUserContext(new Authorizations("a", "b"));
        AccumuloUserContext userAB2 = new AccumuloUserContext(new Authorizations("a", "b"));
        AccumuloUserContext userA1 = new AccumuloUserContext(new Authorizations("a"));
        assertEquals(false, userAB1.equals(userA1));
        assertEquals(true, userAB1.equals(userAB2));
    }
}
