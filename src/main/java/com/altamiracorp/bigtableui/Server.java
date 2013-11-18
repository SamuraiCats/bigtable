package com.altamiracorp.bigtableui;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class.getName());

    @Parameter(names = {"-port"}, description = "The port to run the server on")
    private Integer httpPort = 8000;

    private void run(String[] args) throws Exception {
        new JCommander(this, args);

        SelectChannelConnector httpConnector = new SelectChannelConnector();
        httpConnector.setPort(httpPort);

        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar("./src/main/webapp/");

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{webAppContext});

        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server();
        server.setConnectors(new Connector[]{httpConnector});
        server.setHandler(contexts);
        server.start();
        LOGGER.info("Server running: http://localhost:" + httpPort + "/");
        System.out.println("Server running: http://localhost:" + httpPort + "/");
        server.join();
    }

    public static void main(String[] args) {
        try {
            new Server().run(args);
        } catch (Exception ex) {
            LOGGER.error("Could not run server", ex);
        }
    }
}
