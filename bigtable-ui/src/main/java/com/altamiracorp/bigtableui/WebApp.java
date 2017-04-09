package com.altamiracorp.bigtableui;

import io.lumify.miniweb.App;
import io.lumify.miniweb.Handler;
import com.google.inject.Injector;

import javax.servlet.ServletContext;
import java.util.Enumeration;

public class WebApp extends App {
    private final Injector injector;

    public WebApp(final ServletContext servletConfig, final Injector injector) {
        super(servletConfig);

        this.injector = injector;
        Enumeration initParamNames = servletConfig.getInitParameterNames();
        while (initParamNames.hasMoreElements()) {
            String initParam = (String) initParamNames.nextElement();
            set(initParam, servletConfig.getInitParameter(initParam));
        }
    }

    @Override
    protected Handler[] instantiateHandlers(Class<? extends Handler>[] handlerClasses) throws Exception {
        Handler[] handlers = new Handler[handlerClasses.length];
        for (int i = 0; i < handlerClasses.length; i++) {
            handlers[i] = injector.getInstance(handlerClasses[i]);
        }
        return handlers;
    }
}
