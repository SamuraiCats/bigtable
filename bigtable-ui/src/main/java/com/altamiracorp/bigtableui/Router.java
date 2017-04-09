package com.altamiracorp.bigtableui;

import com.altamiracorp.bigtableui.routes.Query;
import com.altamiracorp.bigtableui.routes.TableGet;
import com.google.inject.Injector;
import io.lumify.miniweb.handlers.StaticFileHandler;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

//import io.lumify.miniweb.StaticFileHandler;

public class Router extends HttpServlet {
    private WebApp app;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        final Injector injector = (Injector) config.getServletContext().getAttribute(Injector.class.getName());

        app = new WebApp(config.getServletContext(), injector);
        app.get("/", new StaticFileHandler(config.getServletContext(), "/index.html"));

        app.get("/table", TableGet.class);
        app.get("/table/{tableName}", Query.class);
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
