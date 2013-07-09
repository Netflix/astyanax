package com.netflix.astyanax.thrift;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.CheckpointManager;
import com.netflix.astyanax.shallows.EmptyCheckpointManager;
import com.netflix.astyanax.thrift.model.ThriftRowsSliceImpl;

public class ThriftAllRowsQueryImpl<K, C> implements AllRowsQuery<K, C> {
    private final static Logger LOG = LoggerFactory.getLogger(ThriftAllRowsQueryImpl.class);
    
    private final ThriftColumnFamilyQueryImpl<K,C> query;
    protected SlicePredicate predicate = new SlicePredicate().setSlice_range(ThriftUtils.createAllInclusiveSliceRange());
    protected CheckpointManager checkpointManager = new EmptyCheckpointManager();
    
    protected ColumnFamily<K, C> columnFamily;
    private ExceptionCallback exceptionCallback;
    private int     blockSize       = 100;
    private boolean repeatLastToken = true;
    private Integer nThreads;
    private String  startToken      ;
    private String  endToken        ;
    private Boolean includeEmptyRows;
    
    public ThriftAllRowsQueryImpl(ThriftColumnFamilyQueryImpl<K, C> query) {
        this.columnFamily = query.columnFamily;
        this.query = query;
    }
    
    protected List<org.apache.cassandra.thrift.KeySlice> getNextBlock(final KeyRange range) {
        ThriftKeyspaceImpl keyspace = query.keyspace;
        
        while (true) {
            try {
                return keyspace.connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<List<org.apache.cassandra.thrift.KeySlice>>(
                                keyspace.tracerFactory.newTracer(CassandraOperationType.GET_ROWS_RANGE, columnFamily),
                                query.pinnedHost, keyspace.getKeyspaceName()) {
                            @Override
                            public List<org.apache.cassandra.thrift.KeySlice> internalExecute(Client client, ConnectionContext context)
                                    throws Exception {
                                
                                List<KeySlice> slice = client.get_range_slices(
                                        new ColumnParent().setColumn_family(columnFamily.getName()), predicate,
                                        range, ThriftConverter.ToThriftConsistencyLevel(query.consistencyLevel));
                                
                                return slice;
                            }

                            @Override
                            public ByteBuffer getRowKey() {
                                if (range.getStart_key() != null)
                                    return range.start_key;
                                return null;
                            }
                        }, query.retry).getResult();
            }
            catch (ConnectionException e) {
                // Let exception callback handle this exception. If it
                // returns false then
                // we return an empty result which the iterator's
                // hasNext() to return false.
                // If no exception handler is provided then simply
                // return an empty set as if the
                // there is no more data
                if (this.getExceptionCallback() == null) {
                    throw new RuntimeException(e);
                }
                else {
                    if (!this.getExceptionCallback().onException(e)) {
                        return new ArrayList<org.apache.cassandra.thrift.KeySlice>();
                    }
                }
            }
        }
    }

    @Override
    public OperationResult<Rows<K, C>> execute() throws ConnectionException {
        return new OperationResultImpl<Rows<K, C>>(Host.NO_HOST, 
                new ThriftAllRowsImpl<K, C>(query.keyspace.getPartitioner(), this, columnFamily), 0);
    }

    @Override
    public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
        throw new UnsupportedOperationException("executeAsync not supported here.  Use execute()");
    }

    private boolean shouldIgnoreEmptyRows() {
        if (getIncludeEmptyRows() == null) {
            if (getPredicate().isSetSlice_range() && getPredicate().getSlice_range().getCount() == 0) {
                return false;
            }
        }
        else {
            return !getIncludeEmptyRows();
        }

        return true;
    }

    @Override
    public void executeWithCallback(final RowCallback<K, C> callback) throws ConnectionException {
        final ThriftKeyspaceImpl keyspace = query.keyspace;
        final Partitioner partitioner = keyspace.getPartitioner();
        final AtomicReference<ConnectionException> error = new AtomicReference<ConnectionException>();
        final boolean bIgnoreTombstones = shouldIgnoreEmptyRows();

        List<Pair<String, String>> ranges;
        if (this.getConcurrencyLevel() != null) {
            ranges = Lists.newArrayList();
            int nThreads = this.getConcurrencyLevel();
            List<TokenRange> tokens = partitioner.splitTokenRange(
                    startToken == null ? partitioner.getMinToken() : startToken, 
                    endToken == null   ? partitioner.getMaxToken() : endToken, 
                    nThreads);
            for (TokenRange range : tokens) {
                try {
                    String currentToken = checkpointManager.getCheckpoint(range.getStartToken());
                    if (currentToken == null) {
                        currentToken = range.getStartToken();
                    }
                    else if (currentToken.equals(range.getEndToken())) {
                        continue;
                    }
                    ranges.add(Pair.create(currentToken, range.getEndToken()));
                } catch (Exception e) {
                    throw ThriftConverter.ToConnectionPoolException(e);
                }
            }
        }
        else {
            ranges = Lists.transform(keyspace.describeRing(true), new Function<TokenRange, Pair<String, String>> () {
                @Override
                public Pair<String, String> apply(TokenRange input) {
                    return Pair.create(input.getStartToken(), input.getEndToken());
                }
            });
        }
        final CountDownLatch doneSignal = new CountDownLatch(ranges.size());

        for (final Pair<String, String> tokenPair : ranges) {
            // Prepare the range of tokens for this token range
            final KeyRange range = new KeyRange()
                    .setCount(getBlockSize())
                    .setStart_token(tokenPair.left)
                    .setEnd_token(tokenPair.right);

            query.executor.submit(new Callable<Void>() {
                private boolean firstBlock = true;
                
                @Override
                public Void call() throws Exception {
                    if (error.get() == null && internalRun()) {
                        query.executor.submit(this);
                    }
                    else {
                        doneSignal.countDown();
                    }
                    return null;
                }

                private boolean internalRun() throws Exception {
                    try {
                        // Get the next block
                        List<KeySlice> ks = keyspace.connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<List<KeySlice>>(keyspace.tracerFactory
                                        .newTracer(CassandraOperationType.GET_ROWS_RANGE,
                                                columnFamily), query.pinnedHost, keyspace
                                        .getKeyspaceName()) {
                                    @Override
                                    public List<KeySlice> internalExecute(Client client, ConnectionContext context)
                                            throws Exception {
                                        return client.get_range_slices(new ColumnParent()
                                                .setColumn_family(columnFamily.getName()),
                                                predicate, range, ThriftConverter
                                                        .ToThriftConsistencyLevel(query.consistencyLevel));
                                    }

                                    @Override
                                    public ByteBuffer getRowKey() {
                                        if (range.getStart_key() != null)
                                            return ByteBuffer.wrap(range.getStart_key());
                                        return null;
                                    }
                                }, query.retry.duplicate()).getResult();

                        // Notify the callback
                        if (!ks.isEmpty()) {
                            KeySlice lastRow = Iterables.getLast(ks);
                            boolean bContinue = (ks.size() == getBlockSize());

                            if (getRepeatLastToken()) {
                                if (firstBlock) {
                                    firstBlock = false;
                                }
                                else {
                                    ks.remove(0);
                                }
                            }
                            
                            if (bIgnoreTombstones) {
                                Iterator<KeySlice> iter = ks.iterator();
                                while (iter.hasNext()) {
                                    if (iter.next().getColumnsSize() == 0)
                                        iter.remove();
                                }
                            }
                            Rows<K, C> rows = new ThriftRowsSliceImpl<K, C>(ks, columnFamily
                                    .getKeySerializer(), columnFamily.getColumnSerializer());
                            try {
                                callback.success(rows);
                            }
                            catch (Throwable t) {
                                ConnectionException ce = ThriftConverter.ToConnectionPoolException(t);
                                error.set(ce);
                                return false;
                            }
                            
                            if (bContinue) {
                                // Determine the start token for the next page
                                String token = partitioner.getTokenForKey(lastRow.bufferForKey()).toString();
                                checkpointManager.trackCheckpoint(tokenPair.left, token);
                                if (getRepeatLastToken()) {
                                    range.setStart_token(partitioner.getTokenMinusOne(token));
                                }
                                else {
                                    range.setStart_token(token);
                                }
                            }
                            else {
                                checkpointManager.trackCheckpoint(tokenPair.left, tokenPair.right);
                                return false;
                            }
                        }
                        else {
                            checkpointManager.trackCheckpoint(tokenPair.left, tokenPair.right);
                            return false;
                        }
                    }
                    catch (Exception e) {
                        ConnectionException ce = ThriftConverter.ToConnectionPoolException(e);
                        if (!callback.failure(ce)) {
                            error.set(ce);
                            return false;
                        }
                    }

                    return true;
                }
            });
        }
        // Block until all threads finish
        try {
            doneSignal.await();
        }
        catch (InterruptedException e) {
            LOG.debug("Execution interrupted on get all rows for keyspace " + keyspace.getKeyspaceName());
        }

        if (error.get() != null) {
            throw error.get();
        }
    }
    
    public AllRowsQuery<K, C> setExceptionCallback(ExceptionCallback cb) {
        exceptionCallback = cb;
        return this;
    }

    protected ExceptionCallback getExceptionCallback() {
        return this.exceptionCallback;
    }

    @Override
    public AllRowsQuery<K, C> setThreadCount(int numberOfThreads) {
        setConcurrencyLevel(numberOfThreads);
        return this;
    }
    
    @Override
    public AllRowsQuery<K, C> setConcurrencyLevel(int numberOfThreads) {
        this.nThreads = numberOfThreads;
        return this;
    }


    @Override
    public AllRowsQuery<K, C> setCheckpointManager(CheckpointManager manager) {
        this.checkpointManager = manager;
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnSlice(C... columns) {
        if (columns != null)
            predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(Arrays.asList(columns)))
                    .setSlice_rangeIsSet(false);
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnSlice(Collection<C> columns) {
        if (columns != null)
            predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(columns)).setSlice_rangeIsSet(
                    false);
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
        predicate.setSlice_range(ThriftUtils.createSliceRange(columnFamily.getColumnSerializer(), startColumn,
                endColumn, reversed, count));
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count) {
        predicate.setSlice_range(new SliceRange(startColumn, endColumn, reversed, count));
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnSlice(ColumnSlice<C> slice) {
        if (slice.getColumns() != null) {
            predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(slice.getColumns()))
                    .setSlice_rangeIsSet(false);
        }
        else {
            predicate.setSlice_range(ThriftUtils.createSliceRange(columnFamily.getColumnSerializer(),
                    slice.getStartColumn(), slice.getEndColumn(), slice.getReversed(), slice.getLimit()));
        }
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnRange(ByteBufferRange range) {
        predicate.setSlice_range(new SliceRange().setStart(range.getStart()).setFinish(range.getEnd())
                .setCount(range.getLimit()).setReversed(range.isReversed()));
        return this;
    }

    @Override
    public AllRowsQuery<K, C> setBlockSize(int blockSize) {
        return setRowLimit(blockSize);
    }

    @Override
    public AllRowsQuery<K, C> setRowLimit(int rowLimit) {
        this.blockSize = rowLimit;
        return this;
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public AllRowsQuery<K, C> setRepeatLastToken(boolean repeatLastToken) {
        this.repeatLastToken = repeatLastToken;
        return this;
    }

    public boolean getRepeatLastToken() {
        return this.repeatLastToken;
    }

    protected Integer getConcurrencyLevel() {
        return this.nThreads;
    }
    
    public AllRowsQuery<K, C> setIncludeEmptyRows(boolean flag) {
        this.includeEmptyRows = flag;
        return this;
    }

    public String getStartToken() {
        return this.startToken;
    }
    
    public String getEndToken() {
        return this.endToken;
    }
    
    @Override
    public AllRowsQuery<K, C> forTokenRange(BigInteger startToken, BigInteger endToken) {
        return forTokenRange(startToken.toString(), endToken.toString());
    }
    
    public AllRowsQuery<K, C> forTokenRange(String startToken, String endToken) {
        this.startToken = startToken;
        this.endToken = endToken;
        return this;
    }
    
    SlicePredicate getPredicate() {
        return predicate;
    }
    
    Boolean getIncludeEmptyRows() {
        return this.includeEmptyRows;
    }
}
