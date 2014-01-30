package com.altamiracorp.bigtableui;

import com.altamiracorp.bigtable.model.ModelSession;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtableui.model.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class BigTableRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(BigTableRepository.class.getName());
    private final ModelSession modelSession;

    @Inject
    public BigTableRepository(final ModelSession modelSession) {
        this.modelSession = modelSession;
    }

    public List<Table> getTables() {
        List<Table> results = new ArrayList<Table>();
        ModelUserContext modelUserContext = modelSession.createModelUserContext();
        List<String> tables = modelSession.getTableList(modelUserContext);
        for (String table : tables) {
            results.add(new Table(table));
        }
        return results;
    }

    public Iterable<Row> query(String tableName, String start, String end, ModelUserContext user) {
        LOGGER.info("query [tableName: " + tableName + ", start: " + start + ", end: " + end + "]");
        return modelSession.findByRowKeyRange(tableName, start, end, user);
    }
}
