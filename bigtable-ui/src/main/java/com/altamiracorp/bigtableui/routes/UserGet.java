package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.bigtableui.security.User;
import com.altamiracorp.miniweb.HandlerChain;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class UserGet extends BaseRequestHandler {
    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        User user = AuthenticationProvider.getUser(request);
        respondWithJson(response, user.toJson());
    }
}
