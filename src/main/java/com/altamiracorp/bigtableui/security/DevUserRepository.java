package com.altamiracorp.bigtableui.security;

import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;
import com.google.inject.Singleton;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Singleton
public class DevUserRepository extends UserRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(DevUserRepository.class.getName());
    private final Properties usersConfig;

    public DevUserRepository() throws IOException {
        usersConfig = new Properties();
        String fileName = "./users.config";
        InputStream in = new FileInputStream(fileName);
        try {
            usersConfig.load(in);
        } finally {
            in.close();
        }
    }

    @Override
    public User validateUser(String username, String password) {
        String expectedPassword = usersConfig.getProperty(username + ".password");
        if (expectedPassword == null) {
            LOGGER.info("Could not find user: " + username);
            return null;
        }
        if (!expectedPassword.equals(password)) {
            LOGGER.info("Invalid password for user: " + username);
            return null;
        }
        String authsStr = usersConfig.getProperty(username + ".auths");
        Authorizations authorizations;
        if (authsStr == null || authsStr.length() == 0) {
            authorizations = new Authorizations();
        } else {
            String[] auths = authsStr.split(",");
            authorizations = new Authorizations(auths);
        }
        return new User(username, new AccumuloUserContext(authorizations));
    }
}
