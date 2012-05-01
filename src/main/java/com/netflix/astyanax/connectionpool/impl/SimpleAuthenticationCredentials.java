package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.astyanax.AuthenticationCredentials;

public class SimpleAuthenticationCredentials implements AuthenticationCredentials {
    private String username;
    private String password;
    private final Map<String, Object> attributes = Maps.newHashMap();

    public SimpleAuthenticationCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public SimpleAuthenticationCredentials setUsername(String username) {
        this.username = username;
        return this;
    }

    public SimpleAuthenticationCredentials setPassword(String password) {
        this.password = password;
        return this;
    }

    public SimpleAuthenticationCredentials setAttribute(String name, Object value) {
        this.attributes.put(name, value);
        return this;
    }

    public SimpleAuthenticationCredentials removeAttribute(String name) {
        this.attributes.remove(name);
        return this;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String[] getAttributeNames() {
        return attributes.keySet().toArray(new String[attributes.size()]);
    }

    @Override
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

}
