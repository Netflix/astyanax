package com.netflix.astyanax;

/**
 * Representation for a user/password used to log into a keyspace.
 * 
 * @author elandau
 * 
 */
public interface AuthenticationCredentials {
    /**
     * The username
     */
    String getUsername();

    /**
     * The password
     */
    String getPassword();

    /**
     * Array of all custom attribute names
     * @return
     */
    String[] getAttributeNames();

    /**
     * Retrieve a single attribute by name
     * 
     * @param name  Attribute name
     */
    Object getAttribute(String name);
}
