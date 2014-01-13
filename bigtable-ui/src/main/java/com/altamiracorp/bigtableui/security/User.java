package com.altamiracorp.bigtableui.security;

import com.altamiracorp.bigtable.model.user.ModelUserContext;
import org.json.JSONObject;

public class User {
    private final String username;
    private final ModelUserContext modelUserContext;

    public User(String username, ModelUserContext modelUserContext) {
        this.username = username;
        this.modelUserContext = modelUserContext;
    }

    public String getUsername() {
        return username;
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("username", getUsername());
        return json;
    }

    public ModelUserContext getModelUserContext() {
        return modelUserContext;
    }
}
