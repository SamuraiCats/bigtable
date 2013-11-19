package com.altamiracorp.bigtableui;

import com.altamiracorp.bigtableui.routes.Query;
import com.altamiracorp.bigtableui.routes.TableGet;
import com.altamiracorp.bigtableui.routes.UserGet;
import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.miniweb.Handler;
import com.altamiracorp.miniweb.StaticFileHandler;
import com.google.inject.Injector;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Router extends HttpServlet {
    private WebApp app;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        final Injector injector = (Injector) config.getServletContext().getAttribute(Injector.class.getName());

        AuthenticationProvider authenticatorInstance = injector.getInstance(AuthenticationProvider.class);
        Class<? extends Handler> authenticator = authenticatorInstance.getClass();

        app = new WebApp(config, injector);
        app.get("/index.html", authenticatorInstance, new StaticFileHandler(config));

        app.get("/user", authenticator, UserGet.class);
        app.get("/table", authenticator, TableGet.class);
        app.get("/table/{tableName}", authenticator, Query.class);
    }

    @Override
    public void service(ServletRequest req, ServletResponse resp) throws ServletException, IOException {
        try {
            HttpServletResponse httpResponse = (HttpServletResponse) resp;
            httpResponse.addHeader("Accept-Ranges", "bytes");
            app.handle((HttpServletRequest) req, httpResponse);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }
}
