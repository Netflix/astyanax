package com.netflix.astyanax.contrib.dualwrites;

/**
 * Interface for dealing with failed writes. 
 * 
 * @author poberai
 *
 */
public interface FailedWritesLogger {

    /**
     * Init resources  (if any required)
     */
    public void init();
    
    /**
     * Log metadata for a failed write
     * @param failedWrite
     */
    public void logFailedWrite(WriteMetadata failedWrite);
    
    /**
     * Clean up any resources allocated
     */
    public void shutdown();
}
