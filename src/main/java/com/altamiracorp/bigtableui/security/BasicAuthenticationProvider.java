package com.altamiracorp.bigtableui.security;

import com.altamiracorp.miniweb.HandlerChain;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.codec.binary.Base64;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Singleton
public class BasicAuthenticationProvider extends AuthenticationProvider {
    private static final String HTTP_BASIC_REALM = "lumify";
    private static final String HTTP_AUTHORIZATION_HEADER = "Authorization";
    private static final String HTTP_AUTHENTICATE_HEADER = "WWW-Authenticate";
    private static final int HTTP_NOT_AUTHORIZED_ERROR_CODE = 401;
    private final UserRepository userRepository;

    @Inject
    public BasicAuthenticationProvider(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        if (isAuthorizationPresent(request)) {
            String username = parseUsernameFromAuthorization(request);
            if (username == null) {
                requestAuthorization(response);
            } else {
                User user = userRepository.findOrAddUser(username);
                if (user == null) {
                    response.sendError(HTTP_NOT_AUTHORIZED_ERROR_CODE);
                    return;
                }
                setUser(request, user);
                chain.next(request, response);
            }
        } else {
            requestAuthorization(response);
        }
    }

    private void requestAuthorization(HttpServletResponse response) throws IOException {
        response.setHeader(HTTP_AUTHENTICATE_HEADER, "Basic realm=\"" + HTTP_BASIC_REALM + "\"");
        response.sendError(HTTP_NOT_AUTHORIZED_ERROR_CODE);
    }

    private String parseUsernameFromAuthorization(HttpServletRequest request) {
        String base64Auth = request.getHeader(HTTP_AUTHORIZATION_HEADER);
        String[] authComponents = base64Auth.split(" ");

        if (authComponents.length == 2 && authComponents[0].equals("Basic")) {
            String usernamePasswordCombo = new String(Base64.decodeBase64(authComponents[1]));
            String[] usernamePassword = usernamePasswordCombo.split(":");
            if (usernamePassword.length == 2) {
                return usernamePassword[0];
            }
        }

        return null;
    }

    private boolean isAuthorizationPresent(HttpServletRequest request) {
        return request.getHeader(HTTP_AUTHORIZATION_HEADER) != null;
    }
}
