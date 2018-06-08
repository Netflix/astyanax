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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that acts as an async logger that helps 'unblock' the caller immediately. 
 * It hands off to a {@link LinkedBlockingQueue} and there is a thread on the other end
 * that polls the queue for tasks which are then actually written using a real logger 
 * provided to this class. 
 * 
 * It leverages the decorator pattern to do this. 
 * 
 * @author poberai
 *
 */
public class AsyncFailedWritesLogger implements FailedWritesLogger {

    private static final Logger Logger = LoggerFactory.getLogger(AsyncFailedWritesLogger.class);
    
    private static final int DEFAULT_QUEUE_SIZE = 1000;

    private final FailedWritesLogger actualWriter;
    private ExecutorService threadPool; 
    private final LinkedBlockingQueue<WriteMetadata> taskQueue;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    
    public AsyncFailedWritesLogger(FailedWritesLogger writer) {
        this(writer, DEFAULT_QUEUE_SIZE);
    }

    public AsyncFailedWritesLogger(FailedWritesLogger writer, int queueSize) {
        this.actualWriter = writer;
        this.taskQueue = new LinkedBlockingQueue<WriteMetadata>(queueSize);
    }
    
    @Override
    public void logFailedWrite(WriteMetadata failedWrite) {
        
        boolean success = taskQueue.offer(failedWrite);
        if (!success) {
            Logger.error("Async failed writes logger is backed up and is dropping failed writes " + failedWrite);
        }
    }
    
    @Override
    public void init() {
        
        if (stop.get()) {
            Logger.error("Will not start async logger, already stopped");
            return;
        }
        
        if (threadPool == null) {
            threadPool = Executors.newScheduledThreadPool(1);
        }
        
        threadPool.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                
                Logger.info("Async failed writes logger starting..");

                while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                    try { 
                        WriteMetadata writeMD = taskQueue.take();  // this is a blocking call
                        try { 
                            actualWriter.logFailedWrite(writeMD);
                        } catch (Exception e) {
                            Logger.error("Failed to log failed write asynchronously", e);
                        }
                    } catch(InterruptedException e) {
                        // stop blocking on the queue and exit
                        stop.set(true);
                    }
                }
                Logger.info("Async failed writes logger exiting..");
                return null;
            }
        });
    }
    
    @Override
    public void shutdown() {
        stop.set(true);
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

}
