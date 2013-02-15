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
package com.netflix.astyanax.util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadEntry;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.WalException;
import com.netflix.astyanax.impl.NoOpWriteAheadLog;

public class WriteAheadMutationBatchExecutor {

    private ListeningExecutorService executor;
    private WriteAheadLog wal = new NoOpWriteAheadLog();
    private Predicate<Exception> retryablePredicate = Predicates.alwaysFalse();
    private final Keyspace keyspace;
    private long waitOnNoHosts = 1000;

    public WriteAheadMutationBatchExecutor(Keyspace keyspace, int nThreads) {
        this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nThreads,
                new ThreadFactoryBuilder().setDaemon(true).build()));
        this.keyspace = keyspace;
    }

    public WriteAheadMutationBatchExecutor(Keyspace keyspace, ExecutorService executor) {
        this.executor = MoreExecutors.listeningDecorator(executor);
        this.keyspace = keyspace;
    }

    public WriteAheadMutationBatchExecutor usingWriteAheadLog(WriteAheadLog wal) {
        this.wal = wal;
        return this;
    }

    public WriteAheadMutationBatchExecutor usingRetryablePredicate(Predicate<Exception> predicate) {
        this.retryablePredicate = predicate;
        return this;
    }

    /**
     * Replay records from the WAL
     */
    public List<ListenableFuture<OperationResult<Void>>> replayWal(int count) {
        List<ListenableFuture<OperationResult<Void>>> futures = Lists.newArrayList();
        WriteAheadEntry walEntry;
        while (null != (walEntry = wal.readNextEntry()) && count-- > 0) {
            MutationBatch m = keyspace.prepareMutationBatch();
            try {
                walEntry.readMutation(m);
                futures.add(executeWalEntry(walEntry, m));
            }
            catch (WalException e) {
                wal.removeEntry(walEntry);
            }
        }
        return futures;
    }

    /**
     * Write a mutation to the wal and execute it
     */
    public ListenableFuture<OperationResult<Void>> execute(final MutationBatch m) throws WalException {
        final WriteAheadEntry walEntry = wal.createEntry();
        walEntry.writeMutation(m);
        return executeWalEntry(walEntry, m);
    }

    private ListenableFuture<OperationResult<Void>> executeWalEntry(final WriteAheadEntry walEntry,
            final MutationBatch m) {
        return executor.submit(new Callable<OperationResult<Void>>() {
            public OperationResult<Void> call() throws Exception {
                try {
                    OperationResult<Void> result = m.execute();
                    wal.removeEntry(walEntry);
                    return result;
                }
                catch (Exception e) {
                    if (e instanceof NoAvailableHostsException) {
                        Thread.sleep(waitOnNoHosts);
                    }
                    if (retryablePredicate.apply(e))
                        executor.submit(this);
                    else
                        wal.removeEntry(walEntry);
                    throw e;
                }
            }
        });
    }

    public void shutdown() {
        executor.shutdown();
    }

}
