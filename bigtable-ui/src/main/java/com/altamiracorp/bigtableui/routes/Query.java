package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtable.model.Column;
import com.altamiracorp.bigtable.model.ColumnFamily;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.Value;
import com.altamiracorp.bigtable.model.accumulo.AccumuloColumn;
import com.altamiracorp.bigtableui.BigTableRepository;
import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.bigtableui.security.User;
import com.altamiracorp.bigtableui.util.StringEscapeUtils;
import com.altamiracorp.miniweb.HandlerChain;
import com.google.inject.Inject;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Query extends BaseRequestHandler {
    public static final int MAX_VALUE_LENGTH = 10000;
    private final BigTableRepository bigTableRepository;

    @Inject
    public Query(final BigTableRepository bigTableRepository) {
        this.bigTableRepository = bigTableRepository;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        User user = AuthenticationProvider.getUser(request);
        final String tableName = (String) request.getAttribute("tableName");
        String start = request.getParameter("start");
        String end = request.getParameter("end");

        if (start == null || start.length() == 0) {
            start = "\u0000";
        }

        if (end == null || end.length() == 0) {
            end = "\uffff";
        }

        start = StringEscapeUtils.unescapeCString(start);
        end = StringEscapeUtils.unescapeCString(end);

        JSONObject json = new JSONObject();
        json.put("tableName", tableName);

        List<Row> rows = this.bigTableRepository.query(tableName, start, end, user.getModelUserContext());
        json.put("rows", rowsToJson(rows));

        respondWithJson(response, json);
    }

    private JSONArray rowsToJson(List<Row> rows) {
        JSONArray result = new JSONArray();
        for (Row row : rows) {
            result.put(rowToJson(row));
        }
        return result;
    }

    private JSONObject rowToJson(Row row) {
        JSONObject result = new JSONObject();
        result.put("key", row.getRowKey().toString());
        result.put("columnFamilies", columnFamiliesToJson(row.getColumnFamilies()));
        return result;
    }

    private JSONObject columnFamiliesToJson(Collection<ColumnFamily> columnFamilies) {
        JSONObject result = new JSONObject();
        for (ColumnFamily columnFamily : columnFamilies) {
            JSONObject columnFamilyJson = new JSONObject();
            columnFamilyJson.put("columns", columnsToJson(columnFamily.getColumns()));
            result.put(columnFamily.getColumnFamilyName(), columnFamilyJson);
        }
        return result;
    }

    private JSONObject columnsToJson(Collection<Column> columns) {
        JSONObject result = new JSONObject();
        for (Column column : columns) {
            result.put(column.getName(), columnToJson(column));
        }
        return result;
    }

    private JSONObject columnToJson(Column column) {
        Value value = column.getValue();
        byte[] v = value.toBytes();
        if (v.length > MAX_VALUE_LENGTH) {
            v = Arrays.copyOfRange(v, 0, 10000);
        }

        JSONObject result = new JSONObject();
        result.put("value", new String(v));
        result.put("length", value.toBytes().length);
        if (column instanceof AccumuloColumn) {
            AccumuloColumn accumuloColumn = (AccumuloColumn) column;
            String columnVisibility = accumuloColumn.getColumnVisibility().toString();
            if (!columnVisibility.equals("[]")) {
                result.put("visibility", columnVisibility);
            }
        }
        return result;
    }
}
