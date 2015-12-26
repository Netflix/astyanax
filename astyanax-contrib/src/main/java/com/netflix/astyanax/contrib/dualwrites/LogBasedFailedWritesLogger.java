package com.netflix.astyanax.contrib.dualwrites;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple impl for {@link FailedWritesLogger} that just logs to the file. 
 * 
 * @author poberai
 *
 */
public class LogBasedFailedWritesLogger implements FailedWritesLogger {

    private static final Logger Logger = LoggerFactory.getLogger(LogBasedFailedWritesLogger.class);
    
    private final AtomicBoolean stop = new AtomicBoolean(false);
    
    @Override
    public void init() {
        Logger.info("-------LOGGER INIT------");
        stop.set(false);
    }

    @Override
    public void logFailedWrite(WriteMetadata failedWrite) {
        if (!stop.get()) {
            Logger.info("FAILED WRITE: " + failedWrite.toString());
        }
    }

    @Override
    public void shutdown() {
        stop.set(true);
        Logger.info("-------LOGGER SHUTDOWN------");
    }

}
