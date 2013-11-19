package com.altamiracorp.bigtableui.security;

import com.altamiracorp.bigtable.model.user.accumulo.AccumuloUserContext;
import com.google.inject.Singleton;

@Singleton
public class DevUserRepository extends UserRepository {
    @Override
    public User findOrAddUser(String username) {
        return new User(username, new AccumuloUserContext());
    }
}
