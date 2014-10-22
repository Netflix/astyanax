package com.netflix.astyanax.contrib.dualwrites;

import java.util.Collection;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Impl of {@link DualWritesStrategy} that writes SEQUENTIALLY to the 2 keyspaces. 
 * first the primary and then the secondary. 
 * 
 * If the 1st keyspace write fails, then the failure is propagated immediately to the caller. 
 * If the 1st one succeeds then the 2nd keyspace is tried as best effort. If the write to the 2nd keyspace fails,
 * then that failed write is given to the provided {@link FailedWritesLogger} which can then decide what to do with it. 
 * 
 * @author poberai
 *
 */
public class BestEffortSecondaryWriteStrategy implements DualWritesStrategy {

    private final FailedWritesLogger failedWritesLogger;
    
    public BestEffortSecondaryWriteStrategy(FailedWritesLogger logger) {
        this.failedWritesLogger = logger;
    }

    @Override
    public <R> Execution<R> wrapExecutions(final Execution<R> primary, final Execution<R> secondary, final Collection<WriteMetadata> writeMetadata) {
        
        return new Execution<R>() {

            @Override
            public OperationResult<R> execute() throws ConnectionException {
                
                OperationResult<R> result = primary.execute();
                try { 
                    secondary.execute();
                } catch (ConnectionException e) {
                    if (failedWritesLogger != null) {
                        for (WriteMetadata writeMD : writeMetadata) {
                            failedWritesLogger.logFailedWrite(writeMD);
                        }
                    }
                }
                return result;
            }

            @Override
            public ListenableFuture<OperationResult<R>> executeAsync() throws ConnectionException {
                throw new RuntimeException("Cannot chain async primary and secondary executions");
            }
        };
    }

    @Override
    public FailedWritesLogger getFailedWritesLogger() {
        return failedWritesLogger;
    }
}
