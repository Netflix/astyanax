package com.netflix.astyanax.contrib.dualwrites;


public interface FailedWritesLogger {

    public void init();
    
    public void logFailedWrite(WriteMetadata failedWrite);
    
    public void shutdown();
}
