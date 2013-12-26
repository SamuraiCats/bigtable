package com.altamiracorp.bigtableui.routes;

import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.bigtableui.security.User;
import com.altamiracorp.bigtableui.security.UserRepository;
import com.altamiracorp.miniweb.HandlerChain;
import com.google.inject.Inject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LogIn extends BaseRequestHandler {
    private final UserRepository userRepository;
    private final AuthenticationProvider authenticationProvider;

    @Inject
    public LogIn(AuthenticationProvider authenticationProvider, UserRepository userRepository) {
        this.authenticationProvider = authenticationProvider;
        this.userRepository = userRepository;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        String username = request.getParameter("username");
        String password = request.getParameter("password");

        User user = this.userRepository.validateUser(username, password);
        authenticationProvider.setUser(request, user);
        response.sendRedirect(response.encodeRedirectURL(request.getContextPath() + "/"));
    }
}
