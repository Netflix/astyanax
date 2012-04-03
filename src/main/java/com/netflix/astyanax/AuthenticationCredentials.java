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
     * 
     * @return
     */
    String getUsername();

    /**
     * The password
     * 
     * @return
     */
    String getPassword();

    /**
     * Array of all custom attribute names
     * 
     * @return
     */
    String[] getAttributeNames();

    /**
     * Retrieve a single attribute by name
     * 
     * @param name
     * @return
     */
    Object getAttribute(String name);
}
