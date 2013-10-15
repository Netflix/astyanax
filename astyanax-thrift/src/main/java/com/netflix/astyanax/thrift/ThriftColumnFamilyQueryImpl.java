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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.netflix.astyanax.shallows.EmptyRowsImpl;
import com.netflix.astyanax.thrift.model.*;

/**
 * Implementation of all column family queries using the thrift API.
 *
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class ThriftColumnFamilyQueryImpl<K, C> implements ColumnFamilyQuery<K, C> {
    private final static Logger LOG = LoggerFactory.getLogger(ThriftColumnFamilyQueryImpl.class);

    final ConnectionPool<Cassandra.Client> connectionPool;
    final ColumnFamily<K, C>               columnFamily;
    final KeyspaceTracerFactory            tracerFactory;
    final ThriftKeyspaceImpl               keyspace;
    ConsistencyLevel                       consistencyLevel;
    final ListeningExecutorService         executor;
    Host                                   pinnedHost;
    RetryPolicy                            retry;

    public ThriftColumnFamilyQueryImpl(ExecutorService executor, KeyspaceTracerFactory tracerFactory,
            ThriftKeyspaceImpl keyspace, ConnectionPool<Cassandra.Client> cp, ColumnFamily<K, C> columnFamily,
            ConsistencyLevel consistencyLevel, RetryPolicy retry) {
        this.keyspace = keyspace;
        this.connectionPool = cp;
        this.consistencyLevel = consistencyLevel;
        this.columnFamily = columnFamily;
        this.tracerFactory = tracerFactory;
        this.executor = MoreExecutors.listeningDecorator(executor);
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
                            public Column<C> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                            public ByteBuffer getRowKey() {
                                return columnFamily.getKeySerializer().toByteBuffer(rowKey);
                            }
                        }, retry);
                    }

                    @Override
                    public ListenableFuture<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
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
                            public ColumnList<C> execute(Client client, ConnectionContext context) throws ConnectionException {
                                if (isPaginating && paginateNoMore) {
                                    return new EmptyColumnList<C>();
                                }

                                return super.execute(client, context);
                            }

                            @Override
                            public ColumnList<C> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                                        if (predicate.getSlice_range().getCount() != Integer.MAX_VALUE)
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
                                        } else if (last.isSetCounter_column()) {
                                            predicate.getSlice_range().setStart(last.getCounter_column().getName());
                                        } else if (last.isSetSuper_column()) {
                                            // TODO: Super columns
                                            // should be deprecated
                                            predicate.getSlice_range().setStart(last.getSuper_column().getName());
                                        } else if (last.isSetCounter_super_column()) {
                                            // TODO: Super columns
                                            // should be deprecated
                                            predicate.getSlice_range().setStart(last.getCounter_super_column().getName());
                                        }
                                    }
                                }
                                ColumnList<C> result = new ThriftColumnOrSuperColumnListImpl<C>(columnList,
                                        columnFamily.getColumnSerializer());
                                return result;
                            }

                            @Override
                            public ByteBuffer getRowKey() {
                                return columnFamily.getKeySerializer().toByteBuffer(rowKey);
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
                            public Integer internalExecute(Client client, ConnectionContext context) throws Exception {
                                return client.get_count(columnFamily.getKeySerializer().toByteBuffer(rowKey),
                                        new ColumnParent().setColumn_family(columnFamily.getName()), predicate,
                                        ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                            }

                            @Override
                            public ByteBuffer getRowKey() {
                                return columnFamily.getKeySerializer().toByteBuffer(rowKey);
                            }
                        }, retry);
                    }

                    @Override
                    public ListenableFuture<OperationResult<Integer>> executeAsync() throws ConnectionException {
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
            public ListenableFuture<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
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
                    private boolean useOriginalTimestamp = true;

                    @Override
                    public OperationResult<Void> execute() throws ConnectionException {
                        return connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<Void>(tracerFactory.newTracer(
                                        CassandraOperationType.COPY_TO, columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client, ConnectionContext context) throws Exception {

                                        long currentTime = keyspace.getConfig().getClock().getCurrentTime();

                                        List<ColumnOrSuperColumn> columnList = client.get_slice(columnFamily
                                                .getKeySerializer().toByteBuffer(rowKey), new ColumnParent()
                                                .setColumn_family(columnFamily.getName()), predicate, ThriftConverter
                                                .ToThriftConsistencyLevel(consistencyLevel));

                                        // Create mutation list from columns in
                                        // the response
                                        List<Mutation> mutationList = new ArrayList<Mutation>();
                                        for (ColumnOrSuperColumn sosc : columnList) {
                                            ColumnOrSuperColumn cosc;

                                            if (sosc.isSetColumn()) {
                                                cosc = new ColumnOrSuperColumn().setColumn(sosc.getColumn());
                                                if (!useOriginalTimestamp)
                                                    cosc.getColumn().setTimestamp(currentTime);
                                            }
                                            else if (sosc.isSetSuper_column()) {
                                                cosc = new ColumnOrSuperColumn().setSuper_column(sosc.getSuper_column());
                                                if (!useOriginalTimestamp) {
                                                    for (org.apache.cassandra.thrift.Column subColumn : sosc.getSuper_column().getColumns()) {
                                                        subColumn.setTimestamp(currentTime);
                                                        subColumn.setTimestamp(currentTime);
                                                    }
                                                }
                                            }
                                            else if (sosc.isSetCounter_column()) {
                                                cosc = new ColumnOrSuperColumn().setCounter_column(sosc.getCounter_column());
                                            }
                                            else if (sosc.isSetCounter_super_column()) {
                                                cosc = new ColumnOrSuperColumn().setCounter_super_column(sosc.getCounter_super_column());
                                            }
                                            else {
                                                continue;
                                            }

                                            mutationList.add(new Mutation().setColumn_or_supercolumn(cosc));
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
                    public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Void>>() {
                            @Override
                            public OperationResult<Void> call() throws Exception {
                                return execute();
                            }
                        });
                    }

                    @Override
                    public RowCopier<K, C> withOriginalTimestamp(boolean useOriginalTimestamp) {
                        this.useOriginalTimestamp = useOriginalTimestamp;
                        return this;
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
                            public Rows<K, C> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                            public ByteBuffer getRowKey() {
                                if (startKey != null)
                                    return columnFamily.getKeySerializer().toByteBuffer(startKey);
                                return null;
                            }
                        }, retry);
            }

            @Override
            public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }

            @Override
            public RowSliceColumnCountQuery<K> getColumnCounts() {
                throw new RuntimeException("Not supported yet");
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
                            public Rows<K, C> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                        }, retry);
            }

            @Override
            public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }

            @Override
            public RowSliceColumnCountQuery<K> getColumnCounts() {
                return new RowSliceColumnCountQuery<K>() {
                    @Override
                    public OperationResult<Map<K, Integer>> execute() throws ConnectionException {
                        return connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<Map<K, Integer>>(tracerFactory.newTracer(
                                        CassandraOperationType.GET_ROWS_SLICE, columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                                    @Override
                                    public Map<K, Integer> internalExecute(Client client, ConnectionContext context) throws Exception {
                                        Map<ByteBuffer, Integer> cfmap = client.multiget_count(
                                                columnFamily.getKeySerializer().toBytesList(keys),
                                                new ColumnParent().setColumn_family(columnFamily.getName()),
                                                predicate,
                                                ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
                                        if (cfmap == null || cfmap.isEmpty()) {
                                            return Maps.newHashMap();
                                        }
                                        else {
                                            return columnFamily.getKeySerializer().fromBytesMap(cfmap);
                                        }
                                    }
                                }, retry);
                    }

                    @Override
                    public ListenableFuture<OperationResult<Map<K, Integer>>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Map<K, Integer>>>() {
                            @Override
                            public OperationResult<Map<K, Integer>> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
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
                            public Rows<K, C> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                            public ByteBuffer getRowKey() {
                                // / return
                                // partitioner.getToken(columnFamily.getKeySerializer().toByteBuffer(keys.iterator().next())).token;
                                return null;
                            }
                        }, retry);
            }

            @Override
            public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                return executor.submit(new Callable<OperationResult<Rows<K, C>>>() {
                    @Override
                    public OperationResult<Rows<K, C>> call() throws Exception {
                        return execute();
                    }
                });
            }

            @Override
            public RowSliceColumnCountQuery<K> getColumnCounts() {
                return new RowSliceColumnCountQuery<K>() {
                    @Override
                    public OperationResult<Map<K, Integer>> execute() throws ConnectionException {
                        return connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<Map<K, Integer>>(tracerFactory.newTracer(
                                        CassandraOperationType.GET_ROWS_SLICE, columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                                    @Override
                                    public Map<K, Integer> internalExecute(Client client, ConnectionContext context) throws Exception {
                                        Map<ByteBuffer, Integer> cfmap = client.multiget_count(columnFamily
                                                .getKeySerializer().toBytesList(keys), new ColumnParent()
                                                .setColumn_family(columnFamily.getName()), predicate, ThriftConverter
                                                .ToThriftConsistencyLevel(consistencyLevel));
                                        if (cfmap == null || cfmap.isEmpty()) {
                                            return Maps.newHashMap();
                                        }
                                        else {
                                            return columnFamily.getKeySerializer().fromBytesMap(cfmap);
                                        }
                                    }

                                    @Override
                                    public ByteBuffer getRowKey() {
                                        // / return
                                        // partitioner.getToken(columnFamily.getKeySerializer().toByteBuffer(keys.iterator().next())).token;
                                        return null;
                                    }
                                }, retry);
                    }

                    @Override
                    public ListenableFuture<OperationResult<Map<K, Integer>>> executeAsync() throws ConnectionException {
                        return executor.submit(new Callable<OperationResult<Map<K, Integer>>>() {
                            @Override
                            public OperationResult<Map<K, Integer>> call() throws Exception {
                                return execute();
                            }
                        });
                    }
                };
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
                            public Rows<K, C> execute(Client client, ConnectionContext context) throws ConnectionException {
                                if (isPaginating && paginateNoMore) {
                                    return new EmptyRowsImpl<K, C>();
                                }

                                return super.execute(client, context);
                            }

                            @Override
                            public Rows<K, C> internalExecute(Client client, ConnectionContext context) throws Exception {
                                List<org.apache.cassandra.thrift.KeySlice> cfmap;
                                cfmap = client.get_indexed_slices(
                                        new ColumnParent().setColumn_family(columnFamily.getName()), indexClause,
                                        predicate, ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));

                                if (cfmap == null) {
                                    return new EmptyRowsImpl<K, C>();
                                }
                                else {
                                    if (isPaginating) {
                                        if (!firstPage && !cfmap.isEmpty() &&
                                              cfmap.get(0).bufferForKey().equals(indexClause.bufferForStart_key())) {
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
            public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
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
        return keyspace.cqlStatementFactory.createCqlQuery(this, cql);
    }

    @Override
    public AllRowsQuery<K, C> getAllRows() {
        return new ThriftAllRowsQueryImpl<K, C>(this);
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

    @Override
    public RowQuery<K, C> getRow(K rowKey) {
        return getKey(rowKey);
    }

    @Override
    public RowSliceQuery<K, C> getRowRange(K startKey, K endKey, String startToken, String endToken, int count) {
        return getKeyRange(startKey, endKey, startToken, endToken, count);
    }

    @Override
    public RowSliceQuery<K, C> getRowSlice(K... keys) {
        return getKeySlice(keys);
    }

    @Override
    public RowSliceQuery<K, C> getRowSlice(Collection<K> keys) {
        return getKeySlice(keys);
    }

    @Override
    public RowSliceQuery<K, C> getRowSlice(Iterable<K> keys) {
        return getKeySlice(keys);
    }
}
