package com.netflix.astyanax;

/**
 * Representation for a user/password used to log into a keyspace.
 * 
 * @author elandau
 * 
 */
public interface AuthenticationCredentials {
    /**
     * @return The username
     */
    String getUsername();

    /**
     * @return The password
     */
    String getPassword();

    /**
     * @return Array of all custom attribute names
     */
    String[] getAttributeNames();

    /**
     * Retrieve a single attribute by name
     * 
     * @param name  Attribute name
     */
    Object getAttribute(String name);
}
