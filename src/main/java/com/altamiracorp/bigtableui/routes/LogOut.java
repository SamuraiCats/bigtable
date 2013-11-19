package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.miniweb.HandlerChain;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LogOut extends BaseRequestHandler {
    private final AuthenticationProvider authenticationProvider;

    public LogOut(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        authenticationProvider.logOut(request, response);
        respondWithHtml(response, "OK");
    }
}
