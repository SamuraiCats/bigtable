package com.altamiracorp.bigtableui;

import com.altamiracorp.bigtableui.security.AuthenticationProvider;
import com.altamiracorp.bigtableui.security.UserRepository;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.lang.reflect.Constructor;

public class ApplicationBootstrap extends AbstractModule implements ServletContextListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationBootstrap.class);
    public static final String CONFIG_AUTHENTICATION_PROVIDER = "AuthenticationProvider";
    public static final String CONFIG_USER_REPOSITORY = "UserRepository";
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
        bind(AuthenticationProvider.class).to(getAuthenticationProviderClass());
        bind(UserRepository.class).to(getUserRepositoryClass());
    }

    private Class<AuthenticationProvider> getAuthenticationProviderClass() {
        return getClassFromConfig(CONFIG_AUTHENTICATION_PROVIDER);
    }

    private Class<UserRepository> getUserRepositoryClass() {
        return getClassFromConfig(CONFIG_USER_REPOSITORY);
    }

    private Object createClassInstanceFromConfig(String configKey) {
        Class clazz = getClassFromConfig(configKey);
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

    private Class getClassFromConfig(String configKey) {
        String className = (String) this.context.getInitParameter(configKey);
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
