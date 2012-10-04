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
package com.netflix.astyanax.thrift;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.Pair;
import org.mortbay.log.Log;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.shallows.EmptyRowsImpl;
import com.netflix.astyanax.thrift.model.ThriftColumnImpl;
import com.netflix.astyanax.thrift.model.ThriftColumnOrSuperColumnListImpl;
import com.netflix.astyanax.thrift.model.ThriftCounterColumnImpl;
import com.netflix.astyanax.thrift.model.ThriftCounterSuperColumnImpl;
import com.netflix.astyanax.thrift.model.ThriftCqlResultImpl;
import com.netflix.astyanax.thrift.model.ThriftCqlRowsImpl;
import com.netflix.astyanax.thrift.model.ThriftRowsListImpl;
import com.netflix.astyanax.thrift.model.ThriftRowsSliceImpl;
import com.netflix.astyanax.thrift.model.ThriftSuperColumnImpl;
import com.netflix.astyanax.util.TokenGenerator;

/**
 * Implementation of all column family queries using the thrift API.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public class ThriftColumnFamilyQueryImpl<K, C> implements ColumnFamilyQuery<K, C> {
    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final ColumnFamily<K, C> columnFamily;
    private final KeyspaceTracerFactory tracerFactory;
    private final Keyspace keyspace;
    private ConsistencyLevel consistencyLevel;
    private final Supplier<IPartitioner> partitioner;
    private final ExecutorService executor;
    private Host pinnedHost;
    private RetryPolicy retry;

    public ThriftColumnFamilyQueryImpl(ExecutorService executor, KeyspaceTracerFactory tracerFactory,
            Keyspace keyspace, ConnectionPool<Cassandra.Client> cp, Supplier<IPartitioner> partitioner,
            ColumnFamily<K, C> columnFamily, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
        this.keyspace = keyspace;
        this.connectionPool = cp;
        this.partitioner = partitioner;
        this.consistencyLevel = consistencyLevel;
        this.columnFamily = columnFamily;
        this.tracerFactory = tracerFactory;
        this.executor = executor;
        this.retry = retry;
    }

    // Single ROW query
    @Override
    public RowQuery<K, C> getKey(final K rowKey) {
        return new AbstractRowQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            private boolean firstPage = true;

            @Override
            public ColumnQuery<C> getColumn(final C column) {
                return new ColumnQuery<C>() {
                    @Override
                    public OperationResult<Column<C>> execute() throws ConnectionException {
                        return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Column<C>>(
                                tracerFactory.newTracer(CassandraOperationType.GET_COLUMN, columnFamily), pinnedHost,
                                keyspace.getKeyspaceName()) {
                            @Override
                            public Column<C> internalExecute(Client client) throws Exception {
                                ColumnOrSuperColumn cosc = client.get(
                                        columnFamily.getKeySerializer().toByteBuffer(rowKey),
                                        new org.apache.cassandra.thrift.ColumnPath().setColumn_family(
                                                columnFamily.getName()).setColumn(
                                                columnFamily.getColumnSerializer().toByteBuffer(column)),
                                        ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                                if (cosc.isSetColumn()) {
                                    org.apache.cassandra.thrift.Column c = cosc.getColumn();
                                    return new ThriftColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(
                                            c.getName()), c);
                                }
                                else if (cosc.isSetSuper_column()) {
                                    // TODO: Super columns
                                    // should be deprecated
                                    SuperColumn sc = cosc.getSuper_column();
                                    return new ThriftSuperColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(
                                            sc.getName()), sc);
                                }
                                else if (cosc.isSetCounter_column()) {
                                    org.apache.cassandra.thrift.CounterColumn c = cosc.getCounter_column();
                                    return new ThriftCounterColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(
                                            c.getName()), c);
                                }
                                else if (cosc.isSetCounter_super_column()) {
                                    // TODO: Super columns
                                    // should be deprecated
                                    CounterSuperColumn sc = cosc.getCounter_super_column();
                                    return new ThriftCounterSuperColumnImpl<C>(columnFamily.getColumnSerializer()
                                            .fromBytes(sc.getName()), sc);
                                }
                                else {
                                    throw new RuntimeException("Unknown column type in response");
                                }
                            }

                            @Override
                            public Token getToken() {
                                return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
                            }
                        }, retry);
                    }

                    @Override
                    public Future<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Column<C>>>() {
                            @Override
                            public OperationResult<Column<C>> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }

            @Override
            public OperationResult<ColumnList<C>> execute() throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<ColumnList<C>>(tracerFactory.newTracer(
                                CassandraOperationType.GET_ROW, columnFamily), pinnedHost, keyspace.getKeyspaceName()) {

                            @Override
                            public ColumnList<C> execute(Client client) throws ConnectionException {
                                if (isPaginating && paginateNoMore) {
                                    return new EmptyColumnList<C>();
                                }

                                return super.execute(client);
                            }

                            @Override
                            public ColumnList<C> internalExecute(Client client) throws Exception {
                                List<ColumnOrSuperColumn> columnList = client.get_slice(columnFamily.getKeySerializer()
                                        .toByteBuffer(rowKey), new ColumnParent().setColumn_family(columnFamily
                                        .getName()), predicate, ThriftConverter
                                        .ToThriftConsistencyLevel(consistencyLevel));

                                // Special handling for pagination
                                if (isPaginating && predicate.isSetSlice_range()) {
                                    // Did we reach the end of the query.
                                    if (columnList.size() != predicate.getSlice_range().getCount()) {
                                        paginateNoMore = true;
                                    }

                                    // If this is the first page then adjust the
                                    // count so we fetch one extra column
                                    // that will later be dropped
                                    if (firstPage) {
                                        firstPage = false;
                                        predicate.getSlice_range().setCount(predicate.getSlice_range().getCount() + 1);
                                    }
                                    else {
                                        if (!columnList.isEmpty())
                                            columnList.remove(0);
                                    }

                                    // Set the start column for the next page to
                                    // the last column of this page.
                                    // We will discard this column later.
                                    if (!columnList.isEmpty()) {
                                        ColumnOrSuperColumn last = Iterables.getLast(columnList);
                                        if (last.isSetColumn()) {
                                            predicate.getSlice_range().setStart(last.getColumn().getName());
                                        }
                                    }
                                }
                                ColumnList<C> result = new ThriftColumnOrSuperColumnListImpl<C>(columnList,
                                        columnFamily.getColumnSerializer());
                                return result;
                            }

                            @Override
                            public Token getToken() {
                                return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
                            }
                        }, retry);
            }

            @Override
            public ColumnCountQuery getCount() {
                return new ColumnCountQuery() {
                    @Override
                    public OperationResult<Integer> execute() throws ConnectionException {
                        return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Integer>(
                                tracerFactory.newTracer(CassandraOperationType.GET_COLUMN_COUNT, columnFamily),
                                pinnedHost, keyspace.getKeyspaceName()) {
                            @Override
                            public Integer internalExecute(Client client) throws Exception {
                                return client.get_count(columnFamily.getKeySerializer().toByteBuffer(rowKey),
                                        new ColumnParent().setColumn_family(columnFamily.getName()), predicate,
                                        ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                            }

                            @Override
                            public Token getToken() {
                                return toToken(columnFamily.getKeySerializer().toByteBuffer(rowKey));
                            }
                        }, retry);
                    }

                    @Override
                    public Future<OperationResult<Integer>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Integer>>() {
                            @Override
                            public OperationResult<Integer> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }

            @Override
            public Future<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<ColumnList<C>>>() {
                    @Override
                    public OperationResult<ColumnList<C>> call() throws Exception {
                        return execute();
                    }
                });
            }

            @Override
            public RowCopier<K, C> copyTo(final ColumnFamily<K, C> otherColumnFamily, final K otherRowKey) {
                return new RowCopier<K, C>() {
                    @Override
                    public OperationResult<Void> execute() throws ConnectionException {
                        return connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<Void>(tracerFactory.newTracer(
                                        CassandraOperationType.COPY_TO, columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client) throws Exception {
                                        List<ColumnOrSuperColumn> columnList = client.get_slice(columnFamily
                                                .getKeySerializer().toByteBuffer(rowKey), new ColumnParent()
                                                .setColumn_family(columnFamily.getName()), predicate, ThriftConverter
                                                .ToThriftConsistencyLevel(consistencyLevel));

                                        // Create mutation list from columns in
                                        // the response
                                        List<Mutation> mutationList = new ArrayList<Mutation>();
                                        for (ColumnOrSuperColumn sosc : columnList) {
                                            Mutation mutation = new Mutation();
                                            mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(sosc
                                                    .getColumn()));
                                            mutationList.add(mutation);
                                        }

                                        // Create mutation map
                                        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                                        HashMap<String, List<Mutation>> cfmap = new HashMap<String, List<Mutation>>();
                                        cfmap.put(otherColumnFamily.getName(), mutationList);
                                        mutationMap.put(columnFamily.getKeySerializer().toByteBuffer(otherRowKey),
                                                cfmap);

                                        // Execute the mutation
                                        client.batch_mutate(mutationMap,
                                                ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                                        return null;
                                    }
                                }, retry);
                    }

                    @Override
                    public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
            }
        };
    }

    @Override
    public RowSliceQuery<K, C> getKeyRange(final K startKey, final K endKey, final String startToken,
            final String endToken, final int count) {
        return new AbstractRowSliceQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<Rows<K, C>>(tracerFactory.newTracer(
                                CassandraOperationType.GET_ROWS_RANGE, columnFamily), pinnedHost, keyspace
                                .getKeyspaceName()) {
                            @Override
                            public Rows<K, C> internalExecute(Client client) throws Exception {
                                // This is a sorted list
                                // Same call for standard and super columns via
                                // the ColumnParent
                                KeyRange range = new KeyRange();
                                if (startKey != null)
                                    range.setStart_key(columnFamily.getKeySerializer().toByteBuffer(startKey));
                                if (endKey != null)
                                    range.setEnd_key(columnFamily.getKeySerializer().toByteBuffer(endKey));
                                range.setCount(count).setStart_token(startToken).setEnd_token(endToken);

                                List<org.apache.cassandra.thrift.KeySlice> keySlices = client.get_range_slices(
                                        new ColumnParent().setColumn_family(columnFamily.getName()), predicate, range,
                                        ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));

                                if (keySlices == null || keySlices.isEmpty()) {
                                    return new EmptyRowsImpl<K, C>();
                                }
                                else {
                                    return new ThriftRowsSliceImpl<K, C>(keySlices, columnFamily.getKeySerializer(),
                                            columnFamily.getColumnSerializer());
                                }
                            }

                            @Override
                            public Token getToken() {
                                if (startKey != null)
                                    return toToken(columnFamily.getKeySerializer().toByteBuffer(startKey));
                                if (startToken != null) {
                                    return toToken(startToken);
                                }
                                return null;
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }
        };
    }
    
    @Override
    public RowSliceQuery<K, C> getKeySlice(final Iterable<K> keys) {
        return new AbstractRowSliceQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<Rows<K, C>>(tracerFactory.newTracer(
                                CassandraOperationType.GET_ROWS_SLICE, columnFamily), pinnedHost, keyspace
                                .getKeyspaceName()) {
                            @Override
                            public Rows<K, C> internalExecute(Client client) throws Exception {
                                Map<ByteBuffer, List<ColumnOrSuperColumn>> cfmap = client.multiget_slice(columnFamily
                                        .getKeySerializer().toBytesList(keys), new ColumnParent()
                                        .setColumn_family(columnFamily.getName()), predicate, ThriftConverter
                                        .ToThriftConsistencyLevel(consistencyLevel));
                                if (cfmap == null || cfmap.isEmpty()) {
                                    return new EmptyRowsImpl<K, C>();
                                }
                                else {
                                    return new ThriftRowsListImpl<K, C>(cfmap, columnFamily.getKeySerializer(),
                                            columnFamily.getColumnSerializer());
                                }
                            }

                            @Override
                            public Token getToken() {
                                // / return
                                // toToken(columnFamily.getKeySerializer().toByteBuffer(keys.iterator().next()));
                                return null;
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }
        };
    }

    @Override
    public RowSliceQuery<K, C> getKeySlice(final K keys[]) {
        return getKeySlice(Arrays.asList(keys));
    }
    
    @Override
    public RowSliceQuery<K, C> getKeySlice(final Collection<K> keys) {
        return new AbstractRowSliceQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<Rows<K, C>>(tracerFactory.newTracer(
                                CassandraOperationType.GET_ROWS_SLICE, columnFamily), pinnedHost, keyspace
                                .getKeyspaceName()) {
                            @Override
                            public Rows<K, C> internalExecute(Client client) throws Exception {
                                Map<ByteBuffer, List<ColumnOrSuperColumn>> cfmap = client.multiget_slice(columnFamily
                                        .getKeySerializer().toBytesList(keys), new ColumnParent()
                                        .setColumn_family(columnFamily.getName()), predicate, ThriftConverter
                                        .ToThriftConsistencyLevel(consistencyLevel));
                                if (cfmap == null || cfmap.isEmpty()) {
                                    return new EmptyRowsImpl<K, C>();
                                }
                                else {
                                    return new ThriftRowsListImpl<K, C>(cfmap, columnFamily.getKeySerializer(),
                                            columnFamily.getColumnSerializer());
                                }
                            }

                            @Override
                            public Token getToken() {
                                // / return
                                // toToken(columnFamily.getKeySerializer().toByteBuffer(keys.iterator().next()));
                                return null;
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }
        };
    }

    @Override
    public ColumnFamilyQuery<K, C> setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    @Override
    public IndexQuery<K, C> searchWithIndex() {
        return new AbstractIndexQueryImpl<K, C>(columnFamily) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<Rows<K, C>>(tracerFactory.newTracer(
                                CassandraOperationType.GET_ROWS_BY_INDEX, columnFamily), pinnedHost, keyspace
                                .getKeyspaceName()) {
                            @Override
                            public Rows<K, C> execute(Client client) throws ConnectionException {
                                if (isPaginating && paginateNoMore) {
                                    return new EmptyRowsImpl<K, C>();
                                }

                                return super.execute(client);
                            }

                            @Override
                            public Rows<K, C> internalExecute(Client client) throws Exception {
                                List<org.apache.cassandra.thrift.KeySlice> cfmap;
                                cfmap = client.get_indexed_slices(
                                        new ColumnParent().setColumn_family(columnFamily.getName()), indexClause,
                                        predicate, ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));

                                if (cfmap == null) {
                                    return new EmptyRowsImpl<K, C>();
                                }
                                else {
                                    if (isPaginating) {
                                        if (!firstPage) {
                                            cfmap.remove(0);
                                        }

                                        try {
                                            if (!cfmap.isEmpty()) {
                                                setNextStartKey(ByteBuffer.wrap(Iterables.getLast(cfmap).getKey()));
                                            }
                                            else {
                                                paginateNoMore = true;
                                            }
                                        }
                                        catch (ArithmeticException e) {
                                            paginateNoMore = true;
                                        }
                                    }
                                    return new ThriftRowsSliceImpl<K, C>(cfmap, columnFamily.getKeySerializer(),
                                            columnFamily.getColumnSerializer());
                                }
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }
        };
    }

    @Override
    public CqlQuery<K, C> withCql(final String cql) {
        return new CqlQuery<K, C>() {
            private boolean useCompression = false;

            @Override
            public OperationResult<CqlResult<K, C>> execute() throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<CqlResult<K, C>>(tracerFactory.newTracer(
                                CassandraOperationType.CQL, columnFamily), pinnedHost, keyspace.getKeyspaceName()) {
                            @Override
                            public CqlResult<K, C> internalExecute(Client client) throws Exception {
                                org.apache.cassandra.thrift.CqlResult res = client.execute_cql_query(StringSerializer
                                        .get().toByteBuffer(cql), useCompression ? Compression.GZIP : Compression.NONE);
                                switch (res.getType()) {
                                case ROWS:
                                    return new ThriftCqlResultImpl<K, C>(new ThriftCqlRowsImpl<K, C>(res.getRows(),
                                            columnFamily.getKeySerializer(), columnFamily.getColumnSerializer()));
                                case INT:
                                    return new ThriftCqlResultImpl<K, C>(res.getNum());
                                default:
                                    return null;
                                }
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<CqlResult<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<CqlResult<K, C>>>() {
                    @Override
                    public OperationResult<CqlResult<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }

            @Override
            public CqlQuery<K, C> useCompression() {
                useCompression = true;
                return this;
            }
        };
    }

    @Override
    public AllRowsQuery<K, C> getAllRows() {
        return new AbstractThriftAllRowsQueryImpl<K, C>(columnFamily) {
            private AbstractThriftAllRowsQueryImpl<K, C> getThisQuery() {
                return this;
            }

            protected List<org.apache.cassandra.thrift.KeySlice> getNextBlock(final KeyRange range) {
                while (true) {
                    try {
                        return connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<List<org.apache.cassandra.thrift.KeySlice>>(
                                        tracerFactory.newTracer(CassandraOperationType.GET_ROWS_RANGE, columnFamily),
                                        pinnedHost, keyspace.getKeyspaceName()) {
                                    @Override
                                    public List<org.apache.cassandra.thrift.KeySlice> internalExecute(Client client)
                                            throws Exception {
                                        return client.get_range_slices(
                                                new ColumnParent().setColumn_family(columnFamily.getName()), predicate,
                                                range, ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                                    }

                                    @Override
                                    public Token getToken() {
                                        if (range.getStart_key() != null)
                                            return toToken(range.start_key);
                                        return toToken(range.getStart_token());
                                    }
                                }, retry).getResult();
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
                        new ThriftAllRowsImpl<K, C>(getThisQuery(), columnFamily, partitioner), 0);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("executeAsync not supported here.  Use execute()");
            }

            @Override
            public void executeWithCallback(final RowCallback<K, C> callback) throws ConnectionException {
                final AtomicReference<ConnectionException> error = new AtomicReference<ConnectionException>();

                final IPartitioner partitioner = ThriftColumnFamilyQueryImpl.this.partitioner.get();

                List<Pair<String, String>> ranges = Lists.newArrayList();
                if (this.getConcurrencyLevel() != null && !partitioner.preservesOrder()) {
                    // Note: ConcurrencyLevel is supported only with the RandomPartitioner for now
                    int nThreads = this.getConcurrencyLevel();
                    for (int i = 0; i < nThreads; i++) {
                        BigIntegerToken start =  new BigIntegerToken(TokenGenerator.initialToken(nThreads, i,   TokenGenerator.MINIMUM, TokenGenerator.MAXIMUM));
                        BigIntegerToken end   =  new BigIntegerToken(TokenGenerator.initialToken(nThreads, i+1, TokenGenerator.MINIMUM, TokenGenerator.MAXIMUM));
                        ranges.add(Pair.create(start.toString(), end.toString()));
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
                
                for (final Pair<String, String> token : ranges) {
                    executor.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            // Prepare the range of tokens for this token range
                            final KeyRange range = new KeyRange().setCount(getBlockSize())
                                    .setStart_token(token.left).setEnd_token(token.right);

                            try {
                                // Loop until we get all the rows for this
                                // token range or we get an exception
                                while (error.get() == null) {
                                    try {
                                        // Get the next block
                                        List<KeySlice> ks = connectionPool.executeWithFailover(
                                                new AbstractKeyspaceOperationImpl<List<KeySlice>>(tracerFactory
                                                        .newTracer(CassandraOperationType.GET_ROWS_RANGE,
                                                                columnFamily), pinnedHost, keyspace
                                                        .getKeyspaceName()) {
                                                    @Override
                                                    public List<KeySlice> internalExecute(Client client)
                                                            throws Exception {
                                                        return client.get_range_slices(new ColumnParent()
                                                                .setColumn_family(columnFamily.getName()),
                                                                predicate, range, ThriftConverter
                                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                                    }

                                                    @Override
                                                    public Token getToken() {
                                                        if (range.getStart_key() != null)
                                                            return toToken(ByteBuffer.wrap(range.getStart_key()));
                                                        return toToken(range.getStart_token());
                                                    }
                                                }, retry.duplicate()).getResult();

                                        // Notify the callback
                                        if (!ks.isEmpty()) {
                                            Rows<K, C> rows = new ThriftRowsSliceImpl<K, C>(ks, columnFamily
                                                    .getKeySerializer(), columnFamily.getColumnSerializer());
                                            callback.success(rows);
                                            if (rows.size() == getBlockSize()) {
                                                Row<K, C> lastRow = rows.getRowByIndex(rows.size() - 1);

                                                // Determine the start token
                                                // for the next page
                                                Token token = partitioner.getToken(lastRow.getRawKey());
                                                if (getRepeatLastToken() && partitioner.preservesOrder()) {
                                                    // Start token is non-inclusive
                                                    token = new BigIntegerToken(
                                                            ((BigIntegerToken) token).token.subtract(BigInteger.ONE));
                                                }
                                                range.setStart_token(partitioner.getTokenFactory().toString(token));
                                            }
                                            else {
                                                return null;
                                            }
                                        }
                                        else {
                                            return null;
                                        }
                                    }
                                    catch (Exception e) {
                                        ConnectionException ce = ThriftConverter.ToConnectionPoolException(e);
                                        if (!callback.failure(ce))   
                                            error.set(ce);
                                    }
                                }
                            }
                            finally {
                                doneSignal.countDown();
                            }
                            return null;
                        }
                    });
                }
                // Block until all threads finish
                try {
                    doneSignal.await();
                }
                catch (InterruptedException e) {
                    Log.debug("Execution interrupted on get all rows for keyspace " + keyspace.getKeyspaceName());
                }

                if (error.get() != null) {
                    throw error.get();
                }
            }
        };
    }

    @Override
    public ColumnFamilyQuery<K, C> pinToHost(Host host) {
        this.pinnedHost = host;
        return this;
    }

    @Override
    public ColumnFamilyQuery<K, C> withRetryPolicy(RetryPolicy retry) {
        this.retry = retry;
        return this;
    }

    private Token toToken(ByteBuffer key) {
        return partitioner.get().getToken(key);
    }

    private Token toToken(String token) {
        return partitioner.get().getTokenFactory().fromString(token);
    }
}
