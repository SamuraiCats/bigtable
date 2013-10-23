package com.altamiracorp.bigtable.model;

import org.json.JSONException;
import org.json.JSONObject;

public class RowKey {
    private final String rowKey;

    public RowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public String toString() {
        return this.rowKey;
    }

    public JSONObject toJson() {
        try {
            JSONObject json = new JSONObject();
            json.put("value", toString());
            return json;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
}
