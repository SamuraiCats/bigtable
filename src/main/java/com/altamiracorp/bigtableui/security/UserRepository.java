package com.altamiracorp.bigtableui.security;

public abstract class UserRepository {
    public abstract User findOrAddUser(String username);
}
