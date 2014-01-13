package com.altamiracorp.bigtableui.security;

import com.altamiracorp.miniweb.HandlerChain;
import com.google.inject.Singleton;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Singleton
public class SessionAuthenticationProvider extends AuthenticationProvider {
    private static final int HTTP_NOT_AUTHORIZED_ERROR_CODE = 401;

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        if (getUser(request) == null) {
            response.sendError(HTTP_NOT_AUTHORIZED_ERROR_CODE);
            return;
        } else {
            chain.next(request, response);
        }
    }

    @Override
    public void logOut(HttpServletRequest request, HttpServletResponse response) {
        setUser(request, null);
    }
}
