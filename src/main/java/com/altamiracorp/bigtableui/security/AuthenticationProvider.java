package com.altamiracorp.bigtableui.security;

import com.altamiracorp.miniweb.Handler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public abstract class AuthenticationProvider implements Handler {
    public static final String CURRENT_USER_REQ_ATTR_NAME = "user.current";

    public void setUser(HttpServletRequest request, User user) {
        request.getSession().setAttribute(AuthenticationProvider.CURRENT_USER_REQ_ATTR_NAME, user);
    }

    public static User getUser(HttpSession session) {
        Object user = session.getAttribute(AuthenticationProvider.CURRENT_USER_REQ_ATTR_NAME);
        return user != null ? (User) user : null;
    }

    public static User getUser(HttpServletRequest request) {
        return AuthenticationProvider.getUser(request.getSession());
    }

    public abstract void logOut(HttpServletRequest request, HttpServletResponse response);
}
