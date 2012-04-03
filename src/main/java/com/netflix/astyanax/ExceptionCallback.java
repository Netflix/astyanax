package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * ExceptionCallback is used in situation where it is not possible to return a
 * checked exception, such as when implementing a custom iterator. The callback
 * implementation
 * 
 * @author elandau
 * 
 */
public interface ExceptionCallback {
    /**
     * 
     * @param e
     * @return true to retry or false to end the iteration
     */
    boolean onException(ConnectionException e);

}
