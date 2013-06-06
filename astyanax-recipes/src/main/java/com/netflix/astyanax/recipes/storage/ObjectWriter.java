/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.recipes.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.util.BlockingConcurrentWindowCounter;

public class ObjectWriter implements Callable<ObjectMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectWriter.class);

    private static final int DEFAULT_CONCURRENCY_LEVEL = 4;
    private static final int MAX_WAIT_TIME_TO_FINISH = 60;
    private static final Integer NO_TTL = null;

    private final ChunkedStorageProvider provider;
    private final String objectName;
    private final InputStream is;
    private int chunkSize;
    private Integer ttl = NO_TTL;
    private String attributes;
    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
    private int maxWaitTimeInSeconds = MAX_WAIT_TIME_TO_FINISH;
    private ObjectWriteCallback callback = new NoOpObjectWriteCallback();

    public ObjectWriter(ChunkedStorageProvider provider, String objectName, InputStream is) {
        this.provider = provider;
        this.objectName = objectName;
        this.chunkSize = provider.getDefaultChunkSize();
        this.is = is;
    }

    public ObjectWriter withChunkSize(int size) {
        this.chunkSize = size;
        return this;
    }

    public ObjectWriter withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }
    
    /**
     * additional attributes (e.g. MD5 hash of the value) 
     * that user want to save along with the meta data
     * @param attributes serialized string (e.g. JSON string)
     */
    public ObjectWriter withAttributes(String attributes) {
        this.attributes = attributes;
        return this;
    }

    public ObjectWriter withConcurrencyLevel(int level) {
        this.concurrencyLevel = level;
        return this;
    }

    public ObjectWriter withMaxWaitTime(int maxWaitTimeInSeconds) {
        this.maxWaitTimeInSeconds = maxWaitTimeInSeconds;
        return this;
    }

    public ObjectWriter withCallback(ObjectWriteCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    public ObjectMetadata call() throws Exception {
        LOG.debug("Writing: " + objectName);

        Preconditions.checkNotNull(objectName, "Must provide a valid object name");
        Preconditions.checkNotNull(is, "Must provide a valid input stream");
        Preconditions.checkNotNull(chunkSize, "Must provide a valid chunkSize");

        final AtomicLong nBytesWritten = new AtomicLong(0);
        final AtomicInteger nChunksWritten = new AtomicInteger(0);
        final AtomicReference<Exception> exception = new AtomicReference<Exception>();

        try {
            final ExecutorService executor = Executors.newFixedThreadPool(concurrencyLevel, new ThreadFactoryBuilder()
                    .setDaemon(true).setNameFormat("ChunkWriter-" + objectName + "-%d").build());
            final BlockingConcurrentWindowCounter chunkCounter = new BlockingConcurrentWindowCounter(concurrencyLevel);
            final AutoAllocatingLinkedBlockingQueue<ByteBuffer> blocks = new AutoAllocatingLinkedBlockingQueue<ByteBuffer>(
                    concurrencyLevel);

            try {
                // Write file data one block at a time
                boolean done = false;
                while (!done && exception.get() == null) {
                    // This throttles us so we don't get too far ahead of
                    // ourselves if one of the threads is stuck
                    final int chunkNumber = chunkCounter.incrementAndGet();

                    // Get a block or allocate a new one
                    final ByteBuffer bb = blocks.poll(new Supplier<ByteBuffer>() {
                        @Override
                        public ByteBuffer get() {
                            return ByteBuffer.allocate(chunkSize);
                        }
                    });

                    // Reset the array and copy some data
                    bb.position(0);
                    int nBytesRead = readFully(is, bb.array(), 0, chunkSize);
                    if (nBytesRead > 0) {
                        bb.limit(nBytesRead);

                        // Send data in a worker thread
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    if (exception.get() == null) {
                                        LOG.debug("WRITE " + chunkNumber + " size=" + bb.limit());
                                        provider.writeChunk(objectName, chunkNumber, bb, ttl);
                                        callback.onChunk(chunkNumber, bb.limit());
                                        nBytesWritten.addAndGet(bb.limit());
                                        nChunksWritten.incrementAndGet();
                                    }
                                }
                                catch (Exception e) {
                                    LOG.error(e.getMessage());
                                    exception.compareAndSet(null, e);
                                    callback.onChunkException(chunkNumber, e);
                                }
                                finally {
                                    blocks.add(bb);
                                    chunkCounter.release(chunkNumber);
                                }
                            }
                        });
                    }
                    else {
                        done = true;
                    }
                }

                // Rethrow any exception we got in a thread
                if (exception.get() != null)
                    throw exception.get();
            }
            finally {
                executor.shutdown();
                if (!executor.awaitTermination(maxWaitTimeInSeconds, TimeUnit.SECONDS)) {
                    throw new Exception("Took too long to write object: " + objectName);
                }
            }

            ObjectMetadata objMetaData = new ObjectMetadata()
                .setChunkCount(nChunksWritten.get())
                .setObjectSize(nBytesWritten.get())
                .setChunkSize(chunkSize)
                .setTtl(ttl)
                .setAttributes(attributes);
            provider.writeMetadata(objectName, objMetaData);
            callback.onSuccess();
            return objMetaData;
        }
        catch (Exception e) {
            callback.onFailure(e);
            LOG.warn(e.getMessage());
            e.printStackTrace();
            try {
                provider.deleteObject(objectName, nChunksWritten.get() + concurrencyLevel);
            }
            catch (Exception e2) {
                LOG.warn(e2.getMessage());
            }
            throw e;
        }
    }

    /**
     * Should switch to IOUtils.read() when we update to the latest version of
     * commons-io
     * 
     * @param in
     * @param b
     * @param off
     * @param len
     * @return
     * @throws IOException
     */
    private static int readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        int total = 0;
        for (;;) {
            int got = in.read(b, off + total, len - total);
            if (got < 0) {
                return (total == 0) ? -1 : total;
            }
            else {
                total += got;
                if (total == len)
                    return total;
            }
        }
    }

}
