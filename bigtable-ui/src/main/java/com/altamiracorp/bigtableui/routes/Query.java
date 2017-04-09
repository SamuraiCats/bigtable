package com.altamiracorp.bigtableui.routes;

import java.util.Arrays;
import java.util.Collection;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import com.altamiracorp.bigtable.model.Column;
import com.altamiracorp.bigtable.model.ColumnFamily;
import com.altamiracorp.bigtable.model.ModelSession;
import com.altamiracorp.bigtable.model.Row;
import com.altamiracorp.bigtable.model.Value;
import com.altamiracorp.bigtable.model.user.ModelUserContext;
import com.altamiracorp.bigtableui.BigTableRepository;
import com.altamiracorp.bigtableui.util.DisplayFormatUtils;
import com.altamiracorp.bigtableui.util.StringEscapeUtils;
import io.lumify.miniweb.HandlerChain;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;

public class Query extends BaseRequestHandler {
    public static final int MAX_VALUE_LENGTH = 10000;
    private final ModelSession modelSession;
    private final BigTableRepository bigTableRepository;

    @Inject
    public Query(
            final ModelSession modelSession,
            final BigTableRepository bigTableRepository) {
        this.modelSession = modelSession;
        this.bigTableRepository = bigTableRepository;
    }

    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        final String tableName = (String) request.getAttribute("tableName");
        String authorizationsCommaSeparated = request.getParameter("authorizations");
        String start = request.getParameter("start");
        String end = request.getParameter("end");
        String rowCountString = request.getParameter("rowCount");

        if (authorizationsCommaSeparated == null) {
            authorizationsCommaSeparated = "";
        }

        if (start == null || start.length() == 0) {
            start = "\u0000";
        }

        if (end == null || end.length() == 0) {
            end = "\uffff";
        }

        if (rowCountString == null) {
            rowCountString = "100";
        }
        long rowCount = Long.parseLong(rowCountString);

        start = StringEscapeUtils.unescapeCString(start);
        end = StringEscapeUtils.unescapeCString(end);

        JSONObject json = new JSONObject();
        json.put("tableName", tableName);

        String[] authorizations = authorizationsCommaSeparated.split(",");
        ModelUserContext modelUserContext = modelSession.createModelUserContext(authorizations);
        Iterable<Row> rows = bigTableRepository.query(tableName, start, end, modelUserContext);
        json.put("rows", rowsToJson(rows, rowCount));

        respondWithJson(response, json);
    }

    private JSONArray rowsToJson(Iterable<Row> rows, long rowCount) {
        JSONArray result = new JSONArray();
        long count = 0;
        for (Row row : rows) {
            result.put(rowToJson(row));
            count++;
            if (count > rowCount) {
                break;
            }
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
        byte[] valueBytes = value.toBytes();
        if (valueBytes.length > MAX_VALUE_LENGTH) {
            //noinspection Since15
            valueBytes = Arrays.copyOfRange(valueBytes, 0, 10000);
        }


        JSONObject result = new JSONObject();
        result.put("value", new String(valueBytes));
        result.put("length", valueBytes.length);
        if (column.getVisibility() != null && !column.getVisibility().equals("[]")) {
            result.put("visibility", column.getVisibility());
        }

        // Provide a hex string of the raw column value for all values
        result.put("rawAsHexString", DisplayFormatUtils.generateHexString(valueBytes));

        // Provide a readable string of the raw column value based on value byte length
        if( valueBytes.length == Ints.BYTES ) {
            result.put("rawAsIntString", DisplayFormatUtils.generateFormattedNumber(value.toInteger()));
        } else if( valueBytes.length == Longs.BYTES && valueBytes.length == Doubles.BYTES ) {
            result.put("rawAsLongString", DisplayFormatUtils.generateFormattedNumber(value.toLong()));
            result.put("rawAsDoubleString", DisplayFormatUtils.generateFormattedDecimal(value.toDouble()));
        }

        return result;
    }
}
