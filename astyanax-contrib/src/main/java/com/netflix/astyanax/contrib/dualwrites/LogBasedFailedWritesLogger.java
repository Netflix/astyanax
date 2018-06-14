/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
