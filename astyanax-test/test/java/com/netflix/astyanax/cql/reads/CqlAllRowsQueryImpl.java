package com.netflix.astyanax.cql.reads;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.reads.model.CqlRangeImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowListImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.CheckpointManager;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.shallows.EmptyCheckpointManager;

/**
 * Impl for {@link AllRowsQuery} that uses the java driver underneath. 
 * Note that it is easier and more intuitive to just use the AllRowsReader recipe instead. 
 * See https://github.com/Netflix/astyanax/wiki/AllRowsReader-All-rows-query for details on how to use the recipe. 
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class CqlAllRowsQueryImpl<K,C> implements AllRowsQuery<K,C> {

    private static final Logger LOG = LoggerFactory.getLogger(CqlAllRowsQueryImpl.class);
    
    private static final Partitioner DEFAULT_PARTITIONER = Murmur3Partitioner.get();
    private final static int DEFAULT_PAGE_SIZE = 100;
    
    private final Keyspace      keyspace;
    private final ColumnFamily<K, C> columnFamily;
    
    private    Integer                 rowLimit = DEFAULT_PAGE_SIZE;
    private    Integer             concurrencyLevel;   // Default to null will force ring describe
    private    ExecutorService     executor;
    private    CheckpointManager   checkpointManager = new EmptyCheckpointManager();
    private    RowCallback<K, C>   rowCallback;
    private    boolean             repeatLastToken;
    private    ColumnSlice<C>      columnSlice;
    private    String              startToken;
    private    String              endToken;
    private    Boolean             includeEmptyRows;  // Default to null will discard tombstones
    private    List<Future<Boolean>> futures = Lists.newArrayList();
    private    AtomicBoolean       cancelling = new AtomicBoolean(false);
    private    Partitioner         partitioner = DEFAULT_PARTITIONER;
    private    ConsistencyLevel	consistencyLevel;
    private    ExceptionCallback   exceptionCallback;
    private AtomicReference<Exception>  error = new AtomicReference<Exception>();

    public CqlAllRowsQueryImpl(Keyspace ks, ColumnFamily<K,C> cf) {
    	this.keyspace = ks;
    	this.columnFamily = cf;
    	
    }
	@Override
	public AllRowsQuery<K, C> setBlockSize(int blockSize) {
		setRowLimit(blockSize);
		return this;
	}

	@Override
	public AllRowsQuery<K, C> setRowLimit(int rowLimit) {
		this.rowLimit = rowLimit;
		return this;
	}

	@Override
	public AllRowsQuery<K, C> setExceptionCallback(ExceptionCallback cb) {
		this.exceptionCallback = cb;
		return this;
	}

	@Override
	public AllRowsQuery<K, C> setCheckpointManager(CheckpointManager manager) {
		this.checkpointManager = manager;
		return this;
	}

	@Override
	public AllRowsQuery<K, C> setRepeatLastToken(boolean condition) {
		this.repeatLastToken = condition;
		return this;
	}

	@Override
	public AllRowsQuery<K, C> setIncludeEmptyRows(boolean flag) {
		this.includeEmptyRows = flag;
		return this;
	}
	@Override
	public AllRowsQuery<K, C>  withColumnSlice(C... columns) {
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public AllRowsQuery<K, C>  withColumnSlice(Collection<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public AllRowsQuery<K, C>  withColumnSlice(ColumnSlice<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public AllRowsQuery<K, C>  withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) columnFamily.getColumnFamilyDefinition();
		String pkColName = cfDef.getPartitionKeyColumnDefinitionList().get(1).getName();
		
		this.columnSlice = new CqlColumnSlice<C>(new CqlRangeBuilder<C>()
				.setColumn(pkColName)
				.setStart(startColumn)
				.setEnd(endColumn)
				.setReversed(reversed)
				.setLimit(count)
				.build());
		return this;
	}

	@Override
	public AllRowsQuery<K, C>  withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int limit) {
		Serializer<C> colSerializer = columnFamily.getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		return this.withColumnRange(start, end, reversed, limit);
	}

	@Override
	public AllRowsQuery<K, C> withColumnRange(ByteBufferRange range) {
		if (range instanceof CqlRangeImpl) {
			this.columnSlice = new CqlColumnSlice<C>();
			((CqlColumnSlice<C>) this.columnSlice).setCqlRange((CqlRangeImpl<C>) range);
			return this;
		} else {
			return this.withColumnRange(range.getStart(), range.getEnd(), range.isReversed(), range.getLimit());
		}
	}

	@Override
	public AllRowsQuery<K, C> setConcurrencyLevel(int numberOfThreads) {
		this.concurrencyLevel = numberOfThreads;
		return this;
	}

	@Override
	@Deprecated
	public AllRowsQuery<K, C> setThreadCount(int numberOfThreads) {
		this.concurrencyLevel = numberOfThreads;
		return this;
	}

	@Override
	public void executeWithCallback(RowCallback<K, C> callback) throws ConnectionException {
		this.rowCallback = callback;
		executeTasks();
	}

	@Override
	public AllRowsQuery<K, C> forTokenRange(BigInteger start, BigInteger end) {
		return forTokenRange(start.toString(), end.toString());
	}

	@Override
	public AllRowsQuery<K, C> forTokenRange(String start, String end) {
		this.startToken = start;
		this.endToken = end;
		return this;
	}
	
	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		
		final AtomicReference<ConnectionException> reference = new AtomicReference<ConnectionException>(null);
		
		final List<Row<K,C>> list = Collections.synchronizedList(new LinkedList<Row<K,C>>());
		
		RowCallback<K,C> rowCallback = new RowCallback<K,C>() {

			@Override
			public void success(Rows<K,C> rows) {
				if (rows != null && !rows.isEmpty()) {
					for (Row<K,C> row : rows) {
						list.add(row);
					}
				}
			}

			@Override
			public boolean failure(ConnectionException e) {
				reference.set(e);
				return false;
			}
		};
		
		executeWithCallback(rowCallback);
		
		if (reference.get() != null) {
			throw reference.get();
		}
		
		CqlRowListImpl<K,C> allRows = new CqlRowListImpl<K,C>(list);
		return new CqlOperationResultImpl<Rows<K,C>>(null, allRows);
	}

	@Override
	public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
		throw new UnsupportedOperationException();
	}


	private Boolean executeTasks() throws ConnectionException {
        error.set(null);
        
        List<Callable<Boolean>> subtasks = Lists.newArrayList();
        
        // We are iterating the entire ring using an arbitrary number of threads
        if (this.concurrencyLevel != null || startToken != null || endToken != null) {

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
            List<TokenRange> ranges = keyspace.describeRing(null, null);
            for (TokenRange range : ranges) {
                if (range.getStartToken().equals(range.getEndToken())) {
                    subtasks.add(makeTokenRangeTask(range.getStartToken(), range.getEndToken()));
                } else {
                    subtasks.add(makeTokenRangeTask(partitioner.getTokenMinusOne(range.getStartToken()), range.getEndToken()));
                }
            }
        }
        
        try {
            // Use a local executor
            if (executor == null) {
                ExecutorService localExecutor = Executors
                        .newFixedThreadPool(subtasks.size(),
                            new ThreadFactoryBuilder().setDaemon(true)
                                .setNameFormat("AstyanaxAllRowsQuery-%d")
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
            
            throw new RuntimeException(error.get());
        }
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
                    
                    int localPageSize = rowLimit;
                    int rowsToSkip = 0;
                    while (!cancelling.get()) {
                        RowSliceQuery<K, C> query = prepareQuery().getKeyRange(null, null, currentToken, endToken, -1);
                        
                        if (columnSlice != null)
                            query.withColumnSlice(columnSlice);
                        
                        Rows<K, C> rows = query.execute().getResult();
                        if (!rows.isEmpty()) {
                           try {
                                if (rowCallback != null) {
                                    try { 
                                    	rowCallback.success(rows);
                                    } catch (Exception e) {
                                    	LOG.error("Failed to process rows", e);
                                        cancel();
                                        return false;
                                    }
                                } else {
                                	LOG.error("Row function is empty");
                                }
                            } catch (Exception e) {
                                error.compareAndSet(null, e);
                                LOG.warn(e.getMessage(), e);
                                cancel();
                                throw new RuntimeException("Error processing row", e);
                            }
                                
                            // Get the next block
                            if (rows.size() == rowLimit) {
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
                                } else {
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
     * Submit all the callables to the executor by synchronize their execution so they all start
     * AFTER the have all been submitted.
     * @param executor
     * @param callables
     * @return
     */
    private List<Future<Boolean>> startTasks(ExecutorService executor, List<Callable<Boolean>> callables) {
        List<Future<Boolean>> tasks = Lists.newArrayList();
        for (Callable<Boolean> callable : callables) {
            tasks.add(executor.submit(callable));
        }
        return tasks;
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
            } catch (Exception e) {
                error.compareAndSet(null, e);
                cancel();
                throw e;
            }
        }
        return true;
    }
    

    
    private ColumnFamilyQuery<K, C> prepareQuery() {
    	ColumnFamilyQuery<K, C> query = keyspace.prepareQuery(columnFamily);
    	if (consistencyLevel != null)
    		query.setConsistencyLevel(consistencyLevel);
    	return query;
    }
    
    
    /**
     * Cancel all pending range iteration tasks.  This will cause all internal threads to exit and
     * call() to return false.
     */
    public synchronized void cancel() {
        cancelling.compareAndSet(false, true);
    }
}
