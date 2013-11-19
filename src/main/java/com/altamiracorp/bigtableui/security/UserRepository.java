package com.altamiracorp.bigtableui.security;

import com.google.inject.Singleton;

public abstract class UserRepository {
    public abstract User findOrAddUser(String username);
}
