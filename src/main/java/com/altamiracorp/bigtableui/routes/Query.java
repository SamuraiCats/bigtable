package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtable.model.Column;
import com.altamiracorp.bigtable.model.ColumnFamily;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.Value;
import com.altamiracorp.bigtableui.BigTableRepository;
import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.bigtableui.security.User;
import com.altamiracorp.miniweb.HandlerChain;
import com.google.inject.Inject;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Collection;
import java.util.List;

public class Query extends BaseRequestHandler {
    private final BigTableRepository bigTableRepository;

    @Inject
    public Query(final BigTableRepository bigTableRepository) {
        this.bigTableRepository = bigTableRepository;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        User user = AuthenticationProvider.getUser(request);
        final String tableName = (String) request.getAttribute("tableName");
        final String start = request.getParameter("start");
        final String end = request.getParameter("end");

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
        result.put("key", toJSONString(row.getRowKey().toString().getBytes()));
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
            result.put(column.getName(), columnValueToJson(column.getValue()));
        }
        return result;
    }

    private String columnValueToJson(Value value) {
        return toJSONString(value.toBytes());
    }

    private String toJSONString(byte[] value) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            byte b = value[i];
            if (b == '"') {
                sb.append("\\\"");
            } else if (b == '\\') {
                sb.append("\\\\");
            } else if (b == '/') {
                sb.append("\\/");
            } else if (b == '\b') {
                sb.append("\\b");
            } else if (b == '\f') {
                sb.append("\\f");
            } else if (b == '\n') {
                sb.append("\\n");
            } else if (b == '\r') {
                sb.append("\\r");
            } else if (b == '\t') {
                sb.append("\\t");
            } else if (b >= ' ' && b <= '~') {
                sb.append((char) b);
            } else {
                sb.append("\\x" + String.format("%02X", b));
            }
        }
        return sb.toString();
    }
}
