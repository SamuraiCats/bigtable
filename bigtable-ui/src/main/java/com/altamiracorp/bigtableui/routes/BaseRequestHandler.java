package com.altamiracorp.bigtableui.routes;

import io.lumify.miniweb.Handler;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public abstract class BaseRequestHandler implements Handler {
    protected void respondWithJson(final HttpServletResponse response, final JSONObject json) throws IOException {
        response.setContentType("application/json");
        response.getWriter().write(json.toString());
    }
}
