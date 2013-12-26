package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.miniweb.HandlerChain;
import com.google.inject.Inject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LogOut extends BaseRequestHandler {
    private final AuthenticationProvider authenticationProvider;

    @Inject
    public LogOut(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        authenticationProvider.logOut(request, response);
        response.sendRedirect(response.encodeRedirectURL(request.getContextPath() + "/"));
    }
}
