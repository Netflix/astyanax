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

import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;

import com.google.common.collect.Iterables;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.CassandraOperationType;
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
import com.netflix.astyanax.thrift.model.*;

/**
 * Implementation of all column family queries using the thrift API.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public class ThriftColumnFamilyQueryImpl<K, C> implements
        ColumnFamilyQuery<K, C> {
    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final ColumnFamily<K, C> columnFamily;
    private final KeyspaceTracerFactory tracerFactory;
    private final Keyspace keyspace;
    private ConsistencyLevel consistencyLevel;
    private static final RandomPartitioner partitioner = new RandomPartitioner();
    private final ExecutorService executor;
    private Host pinnedHost;
    private RetryPolicy retry;

    public ThriftColumnFamilyQueryImpl(ExecutorService executor,
            KeyspaceTracerFactory tracerFactory, Keyspace keyspace,
            ConnectionPool<Cassandra.Client> cp,
            ColumnFamily<K, C> columnFamily, ConsistencyLevel consistencyLevel,
            RetryPolicy retry) {
        this.keyspace = keyspace;
        this.connectionPool = cp;
        this.consistencyLevel = consistencyLevel;
        this.columnFamily = columnFamily;
        this.tracerFactory = tracerFactory;
        this.executor = executor;
        this.retry = retry;
    }

    // Single ROW query
    @Override
    public RowQuery<K, C> getKey(final K rowKey) {
        return new AbstractRowQueryImpl<K, C>(
                columnFamily.getColumnSerializer()) {

            private byte[] previousLastColumnName = new byte[] {};

            @Override
            public ColumnQuery<C> getColumn(final C column) {
                return new ColumnQuery<C>() {
                    @Override
                    public OperationResult<Column<C>> execute()
                            throws ConnectionException {
                        return connectionPool
                                .executeWithFailover(
                                        new AbstractKeyspaceOperationImpl<Column<C>>(
                                                tracerFactory
                                                        .newTracer(
                                                                CassandraOperationType.GET_COLUMN,
                                                                columnFamily),
                                                pinnedHost, keyspace
                                                        .getKeyspaceName()) {
                                            @Override
                                            public Column<C> internalExecute(
                                                    Client client)
                                                    throws Exception {
                                                ColumnOrSuperColumn cosc = client
                                                        .get(columnFamily
                                                                .getKeySerializer()
                                                                .toByteBuffer(
                                                                        rowKey),
                                                                new org.apache.cassandra.thrift.ColumnPath()
                                                                        .setColumn_family(
                                                                                columnFamily
                                                                                        .getName())
                                                                        .setColumn(
                                                                                columnFamily
                                                                                        .getColumnSerializer()
                                                                                        .toByteBuffer(
                                                                                                column)),
                                                                ThriftConverter
                                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                                if (cosc.isSetColumn()) {
                                                    org.apache.cassandra.thrift.Column c = cosc
                                                            .getColumn();
                                                    return new ThriftColumnImpl<C>(
                                                            columnFamily
                                                                    .getColumnSerializer()
                                                                    .fromBytes(
                                                                            c.getName()),
                                                            c);
                                                } else if (cosc
                                                        .isSetSuper_column()) {
                                                    // TODO: Super columns
                                                    // should be deprecated
                                                    SuperColumn sc = cosc
                                                            .getSuper_column();
                                                    return new ThriftSuperColumnImpl<C>(
                                                            columnFamily
                                                                    .getColumnSerializer()
                                                                    .fromBytes(
                                                                            sc.getName()),
                                                            sc);
                                                } else if (cosc
                                                        .isSetCounter_column()) {
                                                    org.apache.cassandra.thrift.CounterColumn c = cosc
                                                            .getCounter_column();
                                                    return new ThriftCounterColumnImpl<C>(
                                                            columnFamily
                                                                    .getColumnSerializer()
                                                                    .fromBytes(
                                                                            c.getName()),
                                                            c);
                                                } else if (cosc
                                                        .isSetCounter_super_column()) {
                                                    // TODO: Super columns
                                                    // should be deprecated
                                                    CounterSuperColumn sc = cosc
                                                            .getCounter_super_column();
                                                    return new ThriftCounterSuperColumnImpl<C>(
                                                            columnFamily
                                                                    .getColumnSerializer()
                                                                    .fromBytes(
                                                                            sc.getName()),
                                                            sc);
                                                } else {
                                                    throw new RuntimeException(
                                                            "Unknown column type in response");
                                                }
                                            }

                                            @Override
                                            public BigInteger getToken() {
                                                return partitioner
                                                        .getToken(columnFamily
                                                                .getKeySerializer()
                                                                .toByteBuffer(
                                                                        rowKey)).token;
                                            }
                                        }, retry);
                    }

                    @Override
                    public Future<OperationResult<Column<C>>> executeAsync()
                            throws ConnectionException {
                        return executor
                                .submit(new Callable<OperationResult<Column<C>>>() {
                                    @Override
                                    public OperationResult<Column<C>> call()
                                            throws Exception {
                                        return execute();
                                    }
                                });
                    }
                };
            }

            @Override
            public OperationResult<ColumnList<C>> execute()
                    throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<ColumnList<C>>(
                                tracerFactory.newTracer(
                                        CassandraOperationType.GET_ROW,
                                        columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                            @Override
                            public ColumnList<C> execute(Client client)
                                    throws ConnectionException {
                                if (isPaginating && paginateNoMore) {
                                    return new EmptyColumnList<C>();
                                }

                                return super.execute(client);
                            }

                            @Override
                            public ColumnList<C> internalExecute(Client client)
                                    throws Exception {
                                List<ColumnOrSuperColumn> columnList = client
                                        .get_slice(
                                                columnFamily.getKeySerializer()
                                                        .toByteBuffer(rowKey),
                                                new ColumnParent()
                                                        .setColumn_family(columnFamily
                                                                .getName()),
                                                predicate,
                                                ThriftConverter
                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                if (predicate.getSlice_range().isReversed()) {
                                    byte[] firstColumnName = columnList.get(0).getColumn().getName();
                                    if (Arrays.equals(firstColumnName, previousLastColumnName)) {
                                        columnList.remove(0);
                                    }
                                }
                                ColumnList<C> result = new ThriftColumnOrSuperColumnListImpl<C>(
                                        columnList, columnFamily
                                                .getColumnSerializer());
                                if (isPaginating && !result.isEmpty()
                                        && predicate.isSetSlice_range()) {
                                    ColumnOrSuperColumn last = Iterables
                                            .getLast(columnList);
                                    if (last.isSetColumn()) {
                                        byte[] currentLastColumnName = last.getColumn().getName();
                                        if (!predicate.getSlice_range().isReversed() || !Arrays.equals(currentLastColumnName, previousLastColumnName)) {
                                            previousLastColumnName = currentLastColumnName;
                                            try {
                                                ByteBuffer nextStart = serializer.getNext(last.getColumn().bufferForName());
                                                predicate.getSlice_range().setStart(nextStart);
                                            } catch (ArithmeticException e) {
                                                paginateNoMore = true;
                                            }
                                        } else {
                                            paginateNoMore = true;
                                            return new EmptyColumnList<C>();
                                        }
                                    }
                                }
                                return result;
                            }

                            @Override
                            public BigInteger getToken() {
                                return partitioner.getToken(columnFamily
                                        .getKeySerializer()
                                        .toByteBuffer(rowKey)).token;
                            }
                        }, retry);
            }

            @Override
            public ColumnCountQuery getCount() {
                return new ColumnCountQuery() {
                    @Override
                    public OperationResult<Integer> execute()
                            throws ConnectionException {
                        return connectionPool
                                .executeWithFailover(
                                        new AbstractKeyspaceOperationImpl<Integer>(
                                                tracerFactory
                                                        .newTracer(
                                                                CassandraOperationType.GET_COLUMN_COUNT,
                                                                columnFamily),
                                                pinnedHost, keyspace
                                                        .getKeyspaceName()) {
                                            @Override
                                            public Integer internalExecute(
                                                    Client client)
                                                    throws Exception {
                                                return client
                                                        .get_count(
                                                                columnFamily
                                                                        .getKeySerializer()
                                                                        .toByteBuffer(
                                                                                rowKey),
                                                                new ColumnParent()
                                                                        .setColumn_family(columnFamily
                                                                                .getName()),
                                                                predicate,
                                                                ThriftConverter
                                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                            }

                                            @Override
                                            public BigInteger getToken() {
                                                return partitioner
                                                        .getToken(columnFamily
                                                                .getKeySerializer()
                                                                .toByteBuffer(
                                                                        rowKey)).token;
                                            }
                                        }, retry);
                    }

                    @Override
                    public Future<OperationResult<Integer>> executeAsync()
                            throws ConnectionException {
                        return executor
                                .submit(new Callable<OperationResult<Integer>>() {
                                    @Override
                                    public OperationResult<Integer> call()
                                            throws Exception {
                                        return execute();
                                    }
                                });
                    }
                };
            }

            @Override
            public Future<OperationResult<ColumnList<C>>> executeAsync()
                    throws ConnectionException {
                return executor
                        .submit(new Callable<OperationResult<ColumnList<C>>>() {
                            @Override
                            public OperationResult<ColumnList<C>> call()
                                    throws Exception {
                                return execute();
                            }
                        });
            }

            @Override
            public RowCopier<K, C> copyTo(
                    final ColumnFamily<K, C> otherColumnFamily,
                    final K otherRowKey) {
                return new RowCopier<K, C>() {
                    @Override
                    public OperationResult<Void> execute()
                            throws ConnectionException {
                        return connectionPool.executeWithFailover(
                                new AbstractKeyspaceOperationImpl<Void>(
                                        tracerFactory.newTracer(
                                                CassandraOperationType.COPY_TO,
                                                columnFamily), pinnedHost,
                                        keyspace.getKeyspaceName()) {
                                    @Override
                                    public Void internalExecute(Client client)
                                            throws Exception {
                                        List<ColumnOrSuperColumn> columnList = client
                                                .get_slice(
                                                        columnFamily
                                                                .getKeySerializer()
                                                                .toByteBuffer(
                                                                        rowKey),
                                                        new ColumnParent()
                                                                .setColumn_family(columnFamily
                                                                        .getName()),
                                                        predicate,
                                                        ThriftConverter
                                                                .ToThriftConsistencyLevel(consistencyLevel));

                                        // Create mutation list from columns in
                                        // the response
                                        List<Mutation> mutationList = new ArrayList<Mutation>();
                                        for (ColumnOrSuperColumn sosc : columnList) {
                                            Mutation mutation = new Mutation();
                                            mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn()
                                                    .setColumn(sosc.getColumn()));
                                            mutationList.add(mutation);
                                        }

                                        // Create mutation map
                                        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                                        HashMap<String, List<Mutation>> cfmap = new HashMap<String, List<Mutation>>();
                                        cfmap.put(otherColumnFamily.getName(),
                                                mutationList);
                                        mutationMap.put(columnFamily
                                                .getKeySerializer()
                                                .toByteBuffer(otherRowKey),
                                                cfmap);

                                        // Execute the mutation
                                        client.batch_mutate(
                                                mutationMap,
                                                ThriftConverter
                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                        return null;
                                    }
                                }, retry);
                    }

                    @Override
                    public Future<OperationResult<Void>> executeAsync()
                            throws ConnectionException {
                        return executor
                                .submit(new Callable<OperationResult<Void>>() {
                                    @Override
                                    public OperationResult<Void> call()
                                            throws Exception {
                                        return execute();
                                    }
                                });
                    }
                };
            }
        };
    }

    @Override
    public RowSliceQuery<K, C> getKeyRange(final K startKey, final K endKey,
            final String startToken, final String endToken, final int count) {
        return new AbstractRowSliceQueryImpl<K, C>(
                columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute()
                    throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<Rows<K, C>>(
                                tracerFactory.newTracer(
                                        CassandraOperationType.GET_ROWS_RANGE,
                                        columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                            @Override
                            public Rows<K, C> internalExecute(Client client)
                                    throws Exception {
                                // This is a sorted list
                                // Same call for standard and super columns via
                                // the ColumnParent
                                KeyRange range = new KeyRange();
                                if (startKey != null)
                                    range.setStart_key(columnFamily
                                            .getKeySerializer().toByteBuffer(
                                                    startKey));
                                if (endKey != null)
                                    range.setEnd_key(columnFamily
                                            .getKeySerializer().toByteBuffer(
                                                    endKey));
                                range.setCount(count)
                                        .setStart_token(startToken)
                                        .setEnd_token(endToken);

                                List<org.apache.cassandra.thrift.KeySlice> keySlices = client
                                        .get_range_slices(
                                                new ColumnParent()
                                                        .setColumn_family(columnFamily
                                                                .getName()),
                                                predicate,
                                                range,
                                                ThriftConverter
                                                        .ToThriftConsistencyLevel(consistencyLevel));

                                if (keySlices == null || keySlices.isEmpty()) {
                                    return new EmptyRowsImpl<K, C>();
                                } else {
                                    return new ThriftRowsSliceImpl<K, C>(
                                            keySlices, columnFamily
                                                    .getKeySerializer(),
                                            columnFamily.getColumnSerializer());
                                }
                            }

                            @Override
                            public BigInteger getToken() {
                                if (startKey != null)
                                    return partitioner.getToken(columnFamily
                                            .getKeySerializer().toByteBuffer(
                                                    startKey)).token;
                                return null;
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync()
                    throws ConnectionException {
                return executor
                        .submit(new Callable<OperationResult<Rows<K, C>>>() {
                            @Override
                            public OperationResult<Rows<K, C>> call()
                                    throws Exception {
                                return execute();
                            }
                        });
            }
        };
    }

    @Override
    public RowSliceQuery<K, C> getKeySlice(K keys[]) {
        return getKeySlice(Arrays.asList(keys));
    }

    @Override
    public RowSliceQuery<K, C> getKeySlice(final Collection<K> keys) {
        return new AbstractRowSliceQueryImpl<K, C>(
                columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute()
                    throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<Rows<K, C>>(
                                tracerFactory.newTracer(
                                        CassandraOperationType.GET_ROWS_SLICE,
                                        columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                            @Override
                            public Rows<K, C> internalExecute(Client client)
                                    throws Exception {
                                Map<ByteBuffer, List<ColumnOrSuperColumn>> cfmap = client
                                        .multiget_slice(
                                                columnFamily.getKeySerializer()
                                                        .toBytesList(keys),
                                                new ColumnParent()
                                                        .setColumn_family(columnFamily
                                                                .getName()),
                                                predicate,
                                                ThriftConverter
                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                if (cfmap == null || cfmap.isEmpty()) {
                                    return new EmptyRowsImpl<K, C>();
                                } else {
                                    return new ThriftRowsListImpl<K, C>(cfmap,
                                            columnFamily.getKeySerializer(),
                                            columnFamily.getColumnSerializer());
                                }
                            }

                            @Override
                            public BigInteger getToken() {
                                // return
                                // partitioner.getToken(columnFamily.getKeySerializer().toByteBuffer(keys.iterator().next())).token;
                                return null;
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync()
                    throws ConnectionException {
                return executor
                        .submit(new Callable<OperationResult<Rows<K, C>>>() {
                            @Override
                            public OperationResult<Rows<K, C>> call()
                                    throws Exception {
                                return execute();
                            }
                        });
            }
        };
    }

    @Override
    public ColumnFamilyQuery<K, C> setConsistencyLevel(
            ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    @Override
    public IndexQuery<K, C> searchWithIndex() {
        return new AbstractIndexQueryImpl<K, C>(columnFamily) {
            @Override
            public OperationResult<Rows<K, C>> execute()
                    throws ConnectionException {
                return connectionPool
                        .executeWithFailover(
                                new AbstractKeyspaceOperationImpl<Rows<K, C>>(
                                        tracerFactory
                                                .newTracer(
                                                        CassandraOperationType.GET_ROWS_BY_INDEX,
                                                        columnFamily),
                                        pinnedHost, keyspace.getKeyspaceName()) {
                                    @Override
                                    public Rows<K, C> execute(Client client)
                                            throws ConnectionException {
                                        if (isPaginating && paginateNoMore) {
                                            return new EmptyRowsImpl<K, C>();
                                        }

                                        return super.execute(client);
                                    }

                                    @Override
                                    public Rows<K, C> internalExecute(
                                            Client client) throws Exception {
                                        List<org.apache.cassandra.thrift.KeySlice> cfmap;
                                        cfmap = client
                                                .get_indexed_slices(
                                                        new ColumnParent()
                                                                .setColumn_family(columnFamily
                                                                        .getName()),
                                                        indexClause,
                                                        predicate,
                                                        ThriftConverter
                                                                .ToThriftConsistencyLevel(consistencyLevel));

                                        if (cfmap == null) {
                                            return new EmptyRowsImpl<K, C>();
                                        } else {
                                            if (isPaginating) {
                                                if (!firstPage) {
                                                    cfmap.remove(0);
                                                }

                                                try {
                                                    if (!cfmap.isEmpty()) {
                                                        setNextStartKey(ByteBuffer
                                                                .wrap(Iterables
                                                                        .getLast(
                                                                                cfmap)
                                                                        .getKey()));
                                                    } else {
                                                        paginateNoMore = true;
                                                    }
                                                } catch (ArithmeticException e) {
                                                    paginateNoMore = true;
                                                }
                                            }
                                            return new ThriftRowsSliceImpl<K, C>(
                                                    cfmap,
                                                    columnFamily
                                                            .getKeySerializer(),
                                                    columnFamily
                                                            .getColumnSerializer());
                                        }
                                    }
                                }, retry);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync()
                    throws ConnectionException {
                return executor
                        .submit(new Callable<OperationResult<Rows<K, C>>>() {
                            @Override
                            public OperationResult<Rows<K, C>> call()
                                    throws Exception {
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
            public OperationResult<CqlResult<K, C>> execute()
                    throws ConnectionException {
                return connectionPool.executeWithFailover(
                        new AbstractKeyspaceOperationImpl<CqlResult<K, C>>(
                                tracerFactory.newTracer(
                                        CassandraOperationType.CQL,
                                        columnFamily), pinnedHost, keyspace
                                        .getKeyspaceName()) {
                            @Override
                            public CqlResult<K, C> internalExecute(Client client)
                                    throws Exception {
                                org.apache.cassandra.thrift.CqlResult res = client
                                        .execute_cql_query(
                                                StringSerializer.get()
                                                        .toByteBuffer(cql),
                                                useCompression ? Compression.GZIP
                                                        : Compression.NONE);
                                switch (res.getType()) {
                                case ROWS:
                                    return new ThriftCqlResultImpl<K, C>(
                                            new ThriftCqlRowsImpl<K, C>(
                                                    res.getRows(),
                                                    columnFamily
                                                            .getKeySerializer(),
                                                    columnFamily
                                                            .getColumnSerializer()));
                                case INT:
                                    return new ThriftCqlResultImpl<K, C>(res
                                            .getNum());
                                default:
                                    return null;
                                }
                            }
                        }, retry);
            }

            @Override
            public Future<OperationResult<CqlResult<K, C>>> executeAsync()
                    throws ConnectionException {
                return executor
                        .submit(new Callable<OperationResult<CqlResult<K, C>>>() {
                            @Override
                            public OperationResult<CqlResult<K, C>> call()
                                    throws Exception {
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

            protected List<org.apache.cassandra.thrift.KeySlice> getNextBlock(
                    final KeyRange range) {
                while (true) {
                    try {
                        return connectionPool
                                .executeWithFailover(
                                        new AbstractKeyspaceOperationImpl<List<org.apache.cassandra.thrift.KeySlice>>(
                                                tracerFactory
                                                        .newTracer(
                                                                CassandraOperationType.GET_ROWS_RANGE,
                                                                columnFamily),
                                                pinnedHost, keyspace
                                                        .getKeyspaceName()) {
                                            @Override
                                            public List<org.apache.cassandra.thrift.KeySlice> internalExecute(
                                                    Client client)
                                                    throws Exception {
                                                return client
                                                        .get_range_slices(
                                                                new ColumnParent()
                                                                        .setColumn_family(columnFamily
                                                                                .getName()),
                                                                predicate,
                                                                range,
                                                                ThriftConverter
                                                                        .ToThriftConsistencyLevel(consistencyLevel));
                                            }

                                            @Override
                                            public BigInteger getToken() {
                                                if (range.getStart_key() != null)
                                                    return partitioner
                                                            .getToken(range.start_key).token;
                                                return null;
                                            }
                                        }, retry).getResult();
                    } catch (ConnectionException e) {
                        // Let exception callback handle this exception. If it
                        // returns false then
                        // we return an empty result which the iterator's
                        // hasNext() to return false.
                        // If no exception handler is provided then simply
                        // return an empty set as if the
                        // there is no more data
                        if (this.getExceptionCallback() == null) {
                            throw new RuntimeException(e);
                        } else {
                            if (!this.getExceptionCallback().onException(e)) {
                                return new ArrayList<org.apache.cassandra.thrift.KeySlice>();
                            }
                        }
                    }
                }
            }

            @Override
            public OperationResult<Rows<K, C>> execute()
                    throws ConnectionException {
                return new OperationResultImpl<Rows<K, C>>(Host.NO_HOST,
                        new ThriftAllRowsImpl<K, C>(getThisQuery(),
                                columnFamily), 0);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync()
                    throws ConnectionException {
                throw new UnsupportedOperationException(
                        "executeAsync not supported here.  Use execute()");
            }

            @Override
            public void executeWithCallback(final RowCallback<K, C> callback)
                    throws ConnectionException {
                List<TokenRange> tokens = keyspace.describeRing();
                final RandomPartitioner partitioner = new RandomPartitioner();

                if (tokens != null) {
                    final CountDownLatch doneSignal = new CountDownLatch(
                            tokens.size());
                    final AtomicReference<ConnectionException> error = new AtomicReference<ConnectionException>();

                    for (final TokenRange token : tokens) {
                        executor.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                // Prepare the range of tokens for this token
                                // range
                                final KeyRange range = new KeyRange()
                                        .setCount(getBlockSize())
                                        .setStart_token(token.getStartToken())
                                        .setEnd_token(token.getEndToken());

                                try {
                                    // Loop until we get all the rows for this
                                    // token range or we get an exception
                                    while (error.get() == null) {
                                        try {
                                            // Get the next block
                                            List<KeySlice> ks = connectionPool
                                                    .executeWithFailover(
                                                            new AbstractKeyspaceOperationImpl<List<KeySlice>>(
                                                                    tracerFactory
                                                                            .newTracer(
                                                                                    CassandraOperationType.GET_ROWS_RANGE,
                                                                                    columnFamily),
                                                                    pinnedHost,
                                                                    keyspace.getKeyspaceName()) {
                                                                @Override
                                                                public List<KeySlice> internalExecute(
                                                                        Client client)
                                                                        throws Exception {
                                                                    return client
                                                                            .get_range_slices(
                                                                                    new ColumnParent()
                                                                                            .setColumn_family(columnFamily
                                                                                                    .getName()),
                                                                                    predicate,
                                                                                    range,
                                                                                    ThriftConverter
                                                                                            .ToThriftConsistencyLevel(consistencyLevel));
                                                                }

                                                                @Override
                                                                public BigInteger getToken() {
                                                                    if (range
                                                                            .getStart_key() != null)
                                                                        return partitioner
                                                                                .getToken(ByteBuffer
                                                                                        .wrap(range
                                                                                                .getStart_key())).token;
                                                                    return null;
                                                                }
                                                            },
                                                            retry.duplicate())
                                                    .getResult();

                                            // Notify the callback
                                            if (!ks.isEmpty()) {
                                                Rows<K, C> rows = new ThriftRowsSliceImpl<K, C>(
                                                        ks,
                                                        columnFamily
                                                                .getKeySerializer(),
                                                        columnFamily
                                                                .getColumnSerializer());
                                                callback.success(rows);
                                                if (rows.size() == getBlockSize()) {
                                                    Row<K, C> lastRow = rows.getRowByIndex(rows
                                                            .size() - 1);

                                                    // Determine the start token
                                                    // for the next page
                                                    String token = partitioner
                                                            .getToken(
                                                                    lastRow.getRawKey())
                                                            .toString();
                                                    if (getRepeatLastToken()) {
                                                        // Start token is
                                                        // non-inclusive
                                                        BigInteger intToken = new BigInteger(
                                                                token)
                                                                .subtract(new BigInteger(
                                                                        "1"));
                                                        range.setStart_token(intToken
                                                                .toString());
                                                    } else {
                                                        range.setStart_token(token);
                                                    }
                                                } else {
                                                    return null;
                                                }
                                            } else {
                                                return null;
                                            }
                                        } catch (Exception e) {
                                            error.set(ThriftConverter
                                                    .ToConnectionPoolException(e));
                                        }
                                    }
                                } finally {
                                    doneSignal.countDown();
                                }
                                return null;
                            }
                        });
                    }

                    // Block until all threads finish
                    try {
                        doneSignal.await();
                    } catch (InterruptedException e) {
                    }

                    if (error.get() != null) {
                        throw error.get();
                    }
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
}
