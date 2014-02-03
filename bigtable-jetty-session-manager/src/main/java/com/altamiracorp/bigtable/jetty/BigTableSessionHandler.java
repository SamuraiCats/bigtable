package com.altamiracorp.bigtable.jetty;

import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.session.SessionHandler;

public class BigTableSessionHandler extends SessionHandler {
    public static final Class TYPE = SessionHandler.class;

    public BigTableSessionHandler() {
        super();
    }

    public BigTableSessionHandler(SessionManager manager) {
        super(manager);
    }
}
