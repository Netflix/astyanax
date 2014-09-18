package com.netflix.astyanax.contrib.dualwrites;

import java.util.Collection;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class BestEffortSecondaryWriteStrategy implements DualWritesStrategy {

    private final FailedWritesLogger failedWritesLogger;
    
    public BestEffortSecondaryWriteStrategy(FailedWritesLogger logger) {
        this.failedWritesLogger = logger;
    }
    
    @Override
    public Execution<Void> wrapExecutions(final Execution<Void> primary, final Execution<Void> secondary, final Collection<WriteMetadata> writeMetadata) {
        
        return new Execution<Void>() {

            @Override
            public OperationResult<Void> execute() throws ConnectionException {
                
                OperationResult<Void> result = primary.execute();
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
            public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
                ListenableFuture<OperationResult<Void>> result = primary.executeAsync();
                try { 
                    secondary.executeAsync();
                } catch (ConnectionException e) {
                    if (failedWritesLogger != null) {
                        for (WriteMetadata writeMD : writeMetadata) {
                            failedWritesLogger.logFailedWrite(writeMD);
                        }
                    }
                }
                return result;
            }
            
        };
    }
}
