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

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;

public class ObjectReader implements Callable<ObjectMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectReader.class);

    private static final int DEFAULT_CONCURRENCY_LEVEL = 4;
    private static final int MAX_WAIT_TIME_TO_FINISH = 60;
    private static final int DEFAULT_BATCH_SIZE = 11;

    private final ChunkedStorageProvider provider;
    private final String objectName;
    private final OutputStream os;

    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
    private int maxWaitTimeInSeconds = MAX_WAIT_TIME_TO_FINISH;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private RetryPolicy retryPolicy;
    private ObjectReadCallback callback = new NoOpObjectReadCallback();

    public ObjectReader(ChunkedStorageProvider provider, String objectName, OutputStream os) {
        this.provider = provider;
        this.objectName = objectName;
        this.os = os;
        this.retryPolicy = new RunOnce();
    }

    public ObjectReader withBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    public ObjectReader withConcurrencyLevel(int level) {
        this.concurrencyLevel = level;
        return this;
    }

    public ObjectReader withRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public ObjectReader withMaxWaitTime(int maxWaitTimeInSeconds) {
        this.maxWaitTimeInSeconds = maxWaitTimeInSeconds;
        return this;
    }

    public ObjectReader withCallback(ObjectReadCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    public ObjectMetadata call() throws Exception {
        LOG.info("Reading: " + objectName);

        Preconditions.checkNotNull(objectName);
        Preconditions.checkNotNull(os);

        try {
            // Try to get the file metadata first. The entire file must be
            // available before it can be downloaded.
            // If not available then we back off and retry using the provided
            // retry policy.
            ObjectMetadata attributes;
            RetryPolicy retry = retryPolicy.duplicate();
            do {
                try {
                    attributes = provider.readMetadata(objectName);
                    if (attributes.isValidForRead())
                        break;
                    if (!retry.allowRetry())
                        throw new NotFoundException("File doesn't exists or isn't ready to be read: " + objectName);
                }
                catch (Exception e) {
                    LOG.warn(e.getMessage());
                    if (!retry.allowRetry())
                        throw e;
                }
            } while (true);

            final AtomicReference<Exception> exception = new AtomicReference<Exception>();
            final AtomicLong totalBytesRead = new AtomicLong();

            // Iterate sequentially building up the batches. Once a complete
            // batch of ids is ready
            // randomize fetching the chunks and then download them in parallel
            List<Integer> idsToRead = Lists.newArrayList();
            for (int block = 0; block < attributes.getChunkCount(); block++) {
                idsToRead.add(block);

                // Got a batch, or reached the end
                if (idsToRead.size() == batchSize || block == attributes.getChunkCount() - 1) {

                    // Read blocks in random order
                    final int firstBlockId = idsToRead.get(0);
                    Collections.shuffle(idsToRead);
                    final AtomicReferenceArray<ByteBuffer> chunks = new AtomicReferenceArray<ByteBuffer>(
                            idsToRead.size());
                    ExecutorService executor = Executors.newFixedThreadPool(
                            concurrencyLevel,
                            new ThreadFactoryBuilder().setDaemon(true)
                                    .setNameFormat("ChunkReader-" + objectName + "-%d").build());
                    try {
                        for (final int chunkId : idsToRead) {
                            executor.submit(new Runnable() {
                                @Override
                                public void run() {
                                    // Do the fetch
                                    RetryPolicy retry = retryPolicy.duplicate();
                                    while (exception.get() == null) {
                                        try {
                                            ByteBuffer chunk = provider.readChunk(objectName, chunkId);
                                            totalBytesRead.addAndGet(chunk.limit());
                                            chunks.set(chunkId - firstBlockId, chunk);
                                            callback.onChunk(chunkId, chunk);
                                            break;
                                        }
                                        catch (Exception e) {
                                            callback.onChunkException(chunkId, e);
                                            if (retry.allowRetry())
                                                continue;
                                            exception.compareAndSet(null, e);
                                        }
                                    }
                                }
                            });
                        }
                    }
                    finally {
                        executor.shutdown();
                        if (!executor.awaitTermination(maxWaitTimeInSeconds, TimeUnit.SECONDS)) {
                            throw new Exception("Took too long to fetch object: " + objectName);
                        }
                    }

                    if (exception.get() != null)
                        throw exception.get();

                    for (int i = 0; i < chunks.length(); i++) {
                        os.write(chunks.get(i).array());
                        os.flush();
                    }
                    idsToRead.clear();
                }
            }

            if (totalBytesRead.get() != attributes.getObjectSize()) {
                throw new Exception("Bytes read (" + totalBytesRead.get() + ") does not match object size ("
                        + attributes.getObjectSize() + ") for object " + objectName);
            }
            callback.onSuccess();
            return attributes;
        }
        catch (Exception e) {
            callback.onFailure(e);
            throw e;
        }
    }
}
