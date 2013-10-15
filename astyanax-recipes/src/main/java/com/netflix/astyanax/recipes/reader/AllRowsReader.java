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
package com.netflix.astyanax.recipes.reader;

import java.io.Flushable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.BigInteger127Partitioner;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.CheckpointManager;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.shallows.EmptyCheckpointManager;
import com.netflix.astyanax.util.Callables;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Recipe that is used to read all rows from a column family.  
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class AllRowsReader<K, C> implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(AllRowsReader.class);
    
    private static final Partitioner DEFAULT_PARTITIONER = BigInteger127Partitioner.get();
    private final static int DEFAULT_PAGE_SIZE = 100;
    
    private final Keyspace      keyspace;
    private final ColumnFamily<K, C> columnFamily;
    
    private final   int                 pageSize;
    private final   Integer             concurrencyLevel;   // Default to null will force ring describe
    private final   ExecutorService     executor;
    private final   CheckpointManager   checkpointManager;
    private final   Function<Row<K,C>, Boolean> rowFunction;
    private final   Function<Rows<K, C>, Boolean> rowsFunction;
    private final   boolean             repeatLastToken;
    private final   ColumnSlice<C>      columnSlice;
    private final   String              startToken;
    private final   String              endToken;
    private final   Boolean             includeEmptyRows;  // Default to null will discard tombstones
    private final   List<Future<Boolean>> futures = Lists.newArrayList();
    private final   AtomicBoolean       cancelling = new AtomicBoolean(false);
    private final   Partitioner         partitioner;
    private final   ConsistencyLevel	consistencyLevel;
    private final   RetryPolicy         retryPolicy;
    private AtomicReference<Exception>  error = new AtomicReference<Exception>();

	private String dc;

	private String rack;
    
    public static class Builder<K, C> {
        private final Keyspace      keyspace;
        private final ColumnFamily<K, C> columnFamily;
        
        private Partitioner         partitioner = DEFAULT_PARTITIONER;
        private int                 pageSize = DEFAULT_PAGE_SIZE;
        private Integer             concurrencyLevel;   // Default to null will force ring describe
        private ExecutorService     executor;
        private CheckpointManager   checkpointManager = new EmptyCheckpointManager();
        private Function<Row<K,C>, Boolean>   rowFunction;
        private Function<Rows<K, C>, Boolean> rowsFunction;
        private boolean             repeatLastToken = true;
        private ColumnSlice<C>      columnSlice;
        private String              startToken;
        private String              endToken;
        private Boolean             includeEmptyRows;  // Default to null will discard tombstones
        private String				dc;
        private String				rack;
        private ConsistencyLevel	consistencyLevel = null;
        private RetryPolicy         retryPolicy;
        
        public Builder(Keyspace ks, ColumnFamily<K, C> columnFamily) {
            this.keyspace     = ks;
            this.columnFamily = columnFamily;
        }
        
        /**
         * Maximum number of rows to return for each incremental query to Cassandra.
         * This limit also represents the page size when paginating.
         * 
         * @param blockSize
         * @return
         */
        public Builder<K, C> withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Use this checkpoint manager to keep track of progress as all rows are being iterated
         * @param manager
         * @return
         */
        public Builder<K, C> withCheckpointManager(CheckpointManager checkpointManager) {
            this.checkpointManager = checkpointManager;
            return this;
        }
        
        /**
         * If true will repeat the last token in the previous block when calling cassandra.  This 
         * feature is off by default and is used to handle situations where different row keys map
         * to the same token value and they are split on a page boundary.  The may not be efficient
         * since it requires the entire row data to be fetched (based on the column slice)
         * 
         * @param repeatLastToken
         * @return
         */
        public Builder<K, C> withRepeatLastToken(boolean repeatLastToken) {
            this.repeatLastToken = repeatLastToken;
            return this;
        }

        /**
         * Specify a non-contiguous set of columns to retrieve.
         * 
         * @param columns
         * @return
         */
        public Builder<K, C> withColumnSlice(C... columns) {
            this.columnSlice = new ColumnSlice<C>(ImmutableList.copyOf(columns));
            return this;
        }

        /**
         * Specify a non-contiguous set of columns to retrieve.
         * 
         * @param columns
         * @return
         */
        public Builder<K, C> withColumnSlice(Collection<C> columns) {
            this.columnSlice = new ColumnSlice<C>(columns);
            return this;
        }

        /**
         * Use this when your application caches the column slice.
         * 
         * @param slice
         * @return
         */
        public Builder<K, C> withColumnSlice(ColumnSlice<C> columns) {
            this.columnSlice = columns;
            return this;
        }

        /**
         * Specify a range of columns to return.
         * 
         * @param startColumn
         *            First column in the range
         * @param endColumn
         *            Last column in the range
         * @param reversed
         *            True if the order should be reversed. Note that for reversed,
         *            startColumn should be greater than endColumn.
         * @param count
         *            Maximum number of columns to return (similar to SQL LIMIT)
         * @return
         */
        public Builder<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
            this.columnSlice = new ColumnSlice<C>(startColumn, endColumn).setReversed(reversed).setLimit(count);
            return this;
        }

        /**
         * Split the query into N threads with each thread processing an equal size chunk from the token range.
         * 
         * Note that the actual number of threads is still limited by the available threads in the thread
         * pool that was set with the AstyanaxConfiguration.
         * 
         * @param numberOfThreads
         * @return
         */
        public Builder<K, C> withConcurrencyLevel(int concurrencyLevel) {
            Preconditions.checkArgument(concurrencyLevel >= 1, "Concurrency level must be >= 1");
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }
        
        /**
         * Use the specific executor for executing the tasks. Note that this should be used with care 
         * when specifying the withConcurrencyLevel. 
         * e.g  if you have a concurrencyLevel of 10 with a fixed threadpool of size 1 then this effectively 
         * negates the point of the concurrencyLevel
         * 
         * @param executor
         * @return
         */
        public Builder<K, C> withExecutor(ExecutorService executor) {
            Preconditions.checkArgument(executor != null, "Supplied executor must not be null");
            this.executor = executor;
            return this;
        }
        
        /**
         * Execute the operation on a specific token range, instead of the entire range.
         * Use this only is combination with setConcurrencyLevel being called otherwise
         * it currently will not have any effect on the query.  When using forTokenRange
         * the specified token range will still be split into the number of threads
         * specified by setConcurrencyLevel
         * 
         * @param startToken
         * @param endToken
         * @return
         */
        public Builder<K, C> withTokenRange(BigInteger startToken, BigInteger endToken) {
            this.startToken = startToken.toString();
            this.endToken   = endToken.toString();
            return this;
        }
        
        public Builder<K, C> withTokenRange(String startToken, String endToken) {
            this.startToken = startToken;
            this.endToken   = endToken;
            return this;
        }
        
        /**
         * Partitioner used to determine token ranges and how to break token ranges
         * into sub parts.  The default is BigInteger127Partitioner which is the
         * RandomPartitioner in cassandra.
         * 
         * @param partitioner
         * @return
         */
        public Builder<K, C> withPartitioner(Partitioner partitioner) {
            this.partitioner = partitioner;
            return this;
        }
        
        /**
         * The default behavior is to exclude empty rows, other than when specifically asking
         * for no columns back.  Setting this to true will result in the row callback function
         * being called for empty rows.
         * @param flag
         * @return
         */
        public Builder<K, C> withIncludeEmptyRows(Boolean flag) {
            this.includeEmptyRows = flag;
            return this;
        }
        
        /**
         * Specify the callback function for each row being read.  This callback must
         * be implemented in a thread safe manner since it will be called by multiple
         * internal threads.
         * @param rowFunction
         * @return
         */
        public Builder<K, C> forEachRow(Function<Row<K,C>, Boolean> rowFunction) {
            this.rowFunction = rowFunction;
            return this;
        }
        
        public Builder<K, C> forEachPage(Function<Rows<K, C>, Boolean> rowsFunction) {
            this.rowsFunction = rowsFunction;
            return this;
        }
        
        public Builder<K, C> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        	this.consistencyLevel = consistencyLevel;
        	return this;
        }
        /**
         * Specify dc to use when auto determining the token ranges to ensure that only ranges
         * in the current dc are used.
         * @param rack
         * @return
         */
        public Builder<K, C> withDc(String dc) {
        	this.dc = dc;
        	return this;
        }
        
        /**
         * Specify rack to use when auto determining the token ranges to ensure that only ranges
         * in the current rack are used.
         * @param rack
         * @return
         */
        public Builder<K,C> withRack(String rack) {
        	this.rack = rack;
        	return this;
        }
        
        public Builder<K,C> withRetryPolicy(RetryPolicy policy) {
            this.retryPolicy = policy;
            return this;
        }
        
        public AllRowsReader<K,C> build() {
            if (partitioner == null) {
                try {
                    partitioner = keyspace.getPartitioner();
                } catch (ConnectionException e) {
                    throw new RuntimeException("Unable to determine partitioner", e);
                }
            }
            return new AllRowsReader<K,C>(keyspace, 
                    columnFamily, 
                    concurrencyLevel, 
                    executor,
                    checkpointManager, 
                    rowFunction, 
                    rowsFunction, 
                    columnSlice, 
                    startToken, 
                    endToken, 
                    includeEmptyRows, 
                    pageSize,
                    repeatLastToken,
                    partitioner,
                    dc,
                    rack,
                    consistencyLevel, 
                    retryPolicy);
        }
    }
    
    public AllRowsReader(Keyspace keyspace, ColumnFamily<K, C> columnFamily, 
            Integer concurrencyLevel, 
            ExecutorService executor,
            CheckpointManager checkpointManager, 
            Function<Row<K, C>, Boolean> rowFunction, 
            Function<Rows<K, C>, Boolean> rowsFunction, 
            ColumnSlice<C> columnSlice,
            String startToken, 
            String endToken, 
            Boolean includeEmptyRows,
            int pageSize,
            boolean repeatLastToken,
            Partitioner partitioner,
            String dc,
            String rack,
            ConsistencyLevel consistencyLevel,
            RetryPolicy retryPolicy) {
        super();
        this.keyspace           = keyspace;
        this.columnFamily       = columnFamily;
        this.concurrencyLevel   = concurrencyLevel;
        this.executor           = executor;
        this.checkpointManager  = checkpointManager;
        this.rowFunction        = rowFunction;
        this.rowsFunction       = rowsFunction;
        this.columnSlice        = columnSlice;
        this.startToken         = startToken;
        this.endToken           = endToken;
        this.pageSize           = pageSize;
        this.repeatLastToken    = repeatLastToken;
        this.partitioner        = partitioner;
        this.dc					= dc;
        this.rack				= rack;
        this.consistencyLevel   = consistencyLevel;
        this.retryPolicy        = retryPolicy;
        
        // Flag explicitly set
        if (includeEmptyRows != null) 
            this.includeEmptyRows = includeEmptyRows;
        // Asking for a column range of size 0
        else if (columnSlice != null && columnSlice.getColumns() == null && columnSlice.getLimit() == 0)
            this.includeEmptyRows = true;
        // Default to false
        else 
            this.includeEmptyRows = false;
    }
    
    private ColumnFamilyQuery<K, C> prepareQuery() {
    	ColumnFamilyQuery<K, C> query = keyspace.prepareQuery(columnFamily);
    	if (consistencyLevel != null)
    		query.setConsistencyLevel(consistencyLevel);
    	if (retryPolicy != null)
    	    query.withRetryPolicy(retryPolicy);
    	return query;
    }

    private Callable<Boolean> makeTokenRangeTask(final String startToken, final String endToken) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() {
                try {
                    String currentToken;
                    try {
                        currentToken = checkpointManager.getCheckpoint(startToken);
                        if (currentToken == null) {
                            currentToken = startToken;
                        }
                        else if (currentToken.equals(endToken)) {
                            return true;
                        }
                    } catch (Exception e) {
                        error.compareAndSet(null, e);
                        LOG.error("Failed to get checkpoint for startToken " + startToken, e);
                        cancel();
                        throw new RuntimeException("Failed to get checkpoint for startToken " + startToken, e);
                    }
                    
                    int localPageSize = pageSize;
                    int rowsToSkip = 0;
                    while (!cancelling.get()) {
                        RowSliceQuery<K, C> query = prepareQuery().getKeyRange(null, null, currentToken, endToken, localPageSize);
                        
                        if (columnSlice != null)
                            query.withColumnSlice(columnSlice);
                        
                        Rows<K, C> rows = query.execute().getResult();
                        if (!rows.isEmpty()) {
                            try {
                                if (rowsFunction != null) {
                                    if (!rowsFunction.apply(rows)) {
                                        cancel();
                                        return false;
                                    }
                                }
                                else {
                                    // Iterate through all the rows and notify the callback function
                                    for (Row<K,C> row : rows) {
                                        if (cancelling.get())
                                            break;
                                        // When repeating the last row, rows to skip will be > 0 
                                        // We skip the rows that were repeated from the previous query
                                        if (rowsToSkip > 0) {
                                            rowsToSkip--;
                                            continue;
                                        }
                                        if (!includeEmptyRows && (row.getColumns() == null || row.getColumns().isEmpty()))
                                            continue;
                                        if (!rowFunction.apply(row)) {
                                            cancel();
                                            return false;
                                        }
                                    }
                                }
                            }
                            catch (Exception e) {
                                error.compareAndSet(null, e);
                                LOG.warn(e.getMessage(), e);
                                cancel();
                                throw new RuntimeException("Error processing row", e);
                            }
                                
                            // Get the next block
                            if (rows.size() == localPageSize) {
                                Row<K, C> lastRow = rows.getRowByIndex(rows.size() - 1);
                                String lastToken = partitioner.getTokenForKey(lastRow.getRawKey());
                                checkpointManager.trackCheckpoint(startToken, currentToken);
                                if (repeatLastToken) {
                                    // Start token is non-inclusive
                                    currentToken = partitioner.getTokenMinusOne(lastToken);
                                    
                                    // Determine the number of rows to skip in the response.  Since we are repeating the
                                    // last token it's possible (although unlikely) that there is more than one key mapping to the
                                    // token.  We therefore count backwards the number of keys that have the same token and skip 
                                    // that number in the next iteration of the loop.  If, for example, 3 keys matched but only 2 were
                                    // returned in this iteration then the first 2 keys will be skipped from the next response.
                                    rowsToSkip = 1;
                                    for (int i = rows.size() - 2; i >= 0; i--, rowsToSkip++) {
                                        if (!lastToken.equals(partitioner.getTokenForKey(rows.getRowByIndex(i).getRawKey()))) {
                                            break;
                                        }
                                    }

                                    if (rowsToSkip == localPageSize) {
                                        localPageSize++;
                                    }
                                }
                                else {
                                    currentToken = lastToken;
                                }
                                
                                continue;
                            }
                        }
                        
                        // We're done!
                        checkpointManager.trackCheckpoint(startToken, endToken);
                        return true;
                    }
                    cancel();
                    return false;
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                    LOG.error("Error process token/key range", e);
                    cancel();
                    throw new RuntimeException("Error process token/key range", e);
                }
            }
        };
    }
    
    /**
     * Main execution block for the all rows query.  
     */
    @Override
    public Boolean call() throws Exception {
        error.set(null);
        
        List<Callable<Boolean>> subtasks = Lists.newArrayList();
        
        // We are iterating the entire ring using an arbitrary number of threads
        if (this.concurrencyLevel != null || startToken != null|| endToken != null) {
            List<TokenRange> tokens = partitioner.splitTokenRange(
                    startToken == null ? partitioner.getMinToken() : startToken, 
                    endToken == null   ? partitioner.getMinToken() : endToken, 
                    this.concurrencyLevel == null ? 1 : this.concurrencyLevel);
            
            for (TokenRange range : tokens) {
                subtasks.add(makeTokenRangeTask(range.getStartToken(), range.getEndToken()));
            }
        }
        // We are iterating through each token range
        else {
            List<TokenRange> ranges = keyspace.describeRing(dc, rack);
            for (TokenRange range : ranges) {
                if (range.getStartToken().equals(range.getEndToken())) 
                    subtasks.add(makeTokenRangeTask(range.getStartToken(), range.getEndToken()));
                else
                    subtasks.add(makeTokenRangeTask(partitioner.getTokenMinusOne(range.getStartToken()), range.getEndToken()));
            }
        }
        
        try {
            // Use a local executor
            if (executor == null) {
                ExecutorService localExecutor = Executors
                        .newFixedThreadPool(subtasks.size(),
                            new ThreadFactoryBuilder().setDaemon(true)
                                .setNameFormat("AstyanaxAllRowsReader-%d")
                                .build());
                
                try {
                    futures.addAll(startTasks(localExecutor, subtasks));
                    return waitForTasksToFinish();
                }
                finally {
                    localExecutor.shutdownNow();
                }
            }
            // Use an externally provided executor
            else {
                futures.addAll(startTasks(executor, subtasks));
                return waitForTasksToFinish();
            }
        }
        catch (Exception e) {
            error.compareAndSet(null, e);
            LOG.warn("AllRowsReader terminated. " + e.getMessage(), e);
            cancel();
            
            throw error.get();
        }
    }
    
    /**
     * Wait for all tasks to finish.
     * 
     * @param futures
     * @return true if all tasks returned true or false otherwise.  
     */
    private boolean waitForTasksToFinish() throws Exception {
        for (Future<Boolean> future : futures) {
            try {
                if (!future.get()) {
                    cancel();
                    return false;
                }
            }
            catch (Exception e) {
                error.compareAndSet(null, e);
                cancel();
                throw e;
            }
        }
        
        if (this.rowFunction instanceof Flushable) {
            ((Flushable)rowFunction).flush();
        }
        return true;
    }
    
    /**
     * Submit all the callables to the executor by synchronize their execution so they all start
     * AFTER the have all been submitted.
     * @param executor
     * @param callables
     * @return
     */
    private List<Future<Boolean>> startTasks(ExecutorService executor, List<Callable<Boolean>> callables) {
        List<Future<Boolean>> tasks = Lists.newArrayList();
        CyclicBarrier barrier = new CyclicBarrier(callables.size());
        for (Callable<Boolean> callable : callables) {
            tasks.add(executor.submit(Callables.decorateWithBarrier(barrier, callable)));
        }
        return tasks;
    }
    
    /**
     * Cancel all pending range iteration tasks.  This will cause all internal threads to exit and
     * call() to return false.
     */
    public synchronized void cancel() {
        cancelling.compareAndSet(false, true);
    }
}
