package com.altamiracorp.bigtable.jetty.model;

import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.RowKey;

public class JettySessionRow extends Row<JettySessionRowKey> {
    public static final String TABLE_NAME = "jetty_session";

    public JettySessionRow(JettySessionRowKey rowKey) {
        super(TABLE_NAME, rowKey);
    }

    public JettySessionRow(RowKey rowKey) {
        super(TABLE_NAME, new JettySessionRowKey(rowKey.toString()));
    }

    public JettySessionRow() {
        super(TABLE_NAME);
    }

    public JettySessionMetadata getMetadata() {
        JettySessionMetadata metadata = get(JettySessionMetadata.COLUMN_FAMILY_NAME);
        if (metadata == null) {
            addColumnFamily(new JettySessionMetadata());
            metadata = get(JettySessionMetadata.COLUMN_FAMILY_NAME);
        }
        return metadata;
    }

    public JettySessionData getData() {
        JettySessionData data = get(JettySessionData.COLUMN_FAMILY_NAME);
        if (data == null) {
            addColumnFamily(new JettySessionData());
            data = get(JettySessionData.COLUMN_FAMILY_NAME);
        }
        return data;
    }
}
