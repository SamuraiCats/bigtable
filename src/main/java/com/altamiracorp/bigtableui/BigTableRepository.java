package com.altamiracorp.bigtableui;

import com.altamiracorp.bigtable.model.ModelSession;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtableui.model.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class BigTableRepository {
    private final ModelSession modelSession;

    @Inject
    public BigTableRepository(final ModelSession modelSession) {
        this.modelSession = modelSession;
    }

    public List<Table> getTables(ModelUserContext user) {
        List<Table> results = new ArrayList<Table>();
        List<String> tables = modelSession.getTableList(user);
        for (String table : tables) {
            results.add(new Table(table));
        }
        return results;
    }

    public List<Row> query(String tableName, String start, String end, ModelUserContext user) {
        return modelSession.findByRowKeyRange(tableName, start, end, user);
    }
}
