package com.altamiracorp.bigtableui.security;

public abstract class UserRepository {
    public abstract User validateUser(String username, String password);
}
