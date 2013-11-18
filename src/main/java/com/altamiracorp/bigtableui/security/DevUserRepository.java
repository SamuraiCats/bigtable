package com.altamiracorp.bigtableui.security;

public class DevUserRepository extends UserRepository {
    @Override
    public User findOrAddUser(String username) {
        return new User(username);
    }
}
