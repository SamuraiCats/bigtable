package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtableui.BigTableRepository;
import com.altamiracorp.bigtableui.model.Table;
import com.google.inject.Inject;
import io.lumify.miniweb.HandlerChain;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

public class TableGet extends BaseRequestHandler {
    private final BigTableRepository bigTableRepository;

    @Inject
    public TableGet(final BigTableRepository bigTableRepository) {
        this.bigTableRepository = bigTableRepository;
    }

    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        JSONObject results = new JSONObject();

        List<Table> tables = bigTableRepository.getTables();
        results.put("tables", tablesToJson(tables));

        respondWithJson(response, results);
    }

    private JSONArray tablesToJson(List<Table> tables) {
        JSONArray tablesJson = new JSONArray();
        for (Table table : tables) {
            JSONObject tableJson = tableToJson(table);
            tablesJson.put(tableJson);
        }
        return tablesJson;
    }

    private JSONObject tableToJson(Table table) {
        JSONObject json = new JSONObject();
        json.put("name", table.getName());
        return json;
    }
}
