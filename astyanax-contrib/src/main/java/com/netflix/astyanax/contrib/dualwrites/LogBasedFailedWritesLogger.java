package com.netflix.astyanax.contrib.dualwrites;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogBasedFailedWritesLogger implements FailedWritesLogger {

    private static final Logger Logger = LoggerFactory.getLogger(LogBasedFailedWritesLogger.class);
    
    private final AtomicBoolean stop = new AtomicBoolean(false);
    
    @Override
    public void init() {
        Logger.info("-------LOGGER INIT------");
        stop.set(true);
    }

    @Override
    public void logFailedWrite(WriteMetadata failedWrite) {
        if (!stop.get()) {
            Logger.info("FAILED WRITE: " + failedWrite.toString());
        }
    }

    @Override
    public void shutdown() {
        stop.set(false);
        Logger.info("-------LOGGER SHUTDOWN------");
    }

}
