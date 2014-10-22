package com.netflix.astyanax.contrib.dualwrites;

import java.util.Collection;

import com.netflix.astyanax.Execution;

/**
 * Interface for dealing with 2 separate executions one is the primary, the other is the secondary. 
 * There are several possible strategies 
 * 
 * 1. BEST EFFORT - Do first, fail everything if it fails. The try second and just log it if it fails.
 * 2. FAIL ON ALL - try both and fail if any of them FAIL. 
 * 3. BEST EFFORT ASYNC - similar to the 1st but try the 2nd write in a separate thread and do not block the caller. 
 * 4. PARALLEL WRITES - similar to 2. but try both in separate threads, so that we don't pay for the penalty of dual writes latency as in the SEQUENTIAL method.
 *  
 * @author poberai
 *
 */
public interface DualWritesStrategy {

    /**
     * 
     * @param primary
     * @param secondary
     * @param writeMetadata
     * @return
     */
    public <R> Execution<R> wrapExecutions(Execution<R> primary, Execution<R> secondary, Collection<WriteMetadata> writeMetadata); 

    /**
     * 
     * @return FailedWritesLogger
     */
    public FailedWritesLogger getFailedWritesLogger();
}
