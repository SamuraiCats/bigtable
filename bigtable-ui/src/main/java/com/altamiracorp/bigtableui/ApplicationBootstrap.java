package com.altamiracorp.bigtableui;

import com.altamiracorp.bigtable.model.ModelSession;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

public class ApplicationBootstrap extends AbstractModule implements ServletContextListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationBootstrap.class);
    public static final String CONFIG_MODEL_SESSION = "bigtable.modelSession";
    private ServletContext context;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        LOGGER.info("Servlet context initialized...");

        final ServletContext context = sce.getServletContext();

        if (context != null) {
            this.context = context;
            final Injector injector = Guice.createInjector(this);

            // Store the injector in the context for a servlet to access later
            context.setAttribute(Injector.class.getName(), injector);
        } else {
            LOGGER.error("Servlet context could not be acquired!");
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
    }

    @Override
    protected void configure() {
        Map modelConfig = loadModelConfig();
        bind(ModelSession.class).toInstance(createModelSession(modelConfig));
    }

    private ModelSession createModelSession(Map modelConfig) {
        ModelSession modelSession = (ModelSession) createClassInstanceFromConfig(modelConfig, CONFIG_MODEL_SESSION);
        modelSession.init(modelConfig);
        return modelSession;
    }

    private Map loadModelConfig() {
        String fileName = getModelPropertiesFileName();
        if (fileName == null || !new File(fileName).isFile()) {
            fileName = "/opt/bigtable-ui/config/model.config";
        }
        try {
            Properties properties = new Properties();
            InputStream in = new FileInputStream(fileName);
            try {
                properties.load(in);
            } finally {
                in.close();
            }
            return properties;
        } catch (IOException ex) {
            throw new RuntimeException("Could not load properties from " + fileName, ex);
        }
    }

    private String getModelPropertiesFileName() {
        return context.getInitParameter("ModelPropertiesFileName");
    }

    private Object createClassInstanceFromConfig(Map modelConfig, String configKey) {
        Class clazz = getClassFromConfig(modelConfig, configKey);
        try {
            Constructor constructor = clazz.getConstructor();
            try {
                return constructor.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Could not instantiate class: " + clazz.getName());
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find default constructor for class: " + clazz.getName());
        }
    }

    private Class getClassFromConfig(Map modelConfig, String configKey) {
        String className = (String) modelConfig.get(configKey);
        if (className == null) {
            throw new RuntimeException("Could not find config: " + configKey);
        }
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not create class: " + className, e);
        }
    }
}
