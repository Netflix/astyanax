package com.netflix.astyanax.connectionpool.impl;

public interface HostStats {

    /**
     * Get number of borrowed connections
     * @return
     */
    long getBorrowedCount();

    /**
     * Get number of returned connections
     * @return
     */
    long getReturnedCount();
    
    /**
     * Get number of successful operations
     * @return
     */
    long getSuccessCount();
    
    /**
     * Get number of failed operations
     * @return
     */
    long getErrorCount();
}
