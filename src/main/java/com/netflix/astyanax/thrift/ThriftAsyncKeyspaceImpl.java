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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.KeySlice;
import com.netflix.astyanax.model.TokenRange;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.shallows.EmptyRowsImpl;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncMethodCall;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@SuppressWarnings({"ConstantConditions"})
public final class ThriftAsyncKeyspaceImpl implements Keyspace {

	private final ConnectionPool<Cassandra.AsyncClient> connectionPool;
	private final RandomPartitioner partitioner;
	private final NodeDiscovery discovery;
	private final AstyanaxConfiguration config;
	private final KeyspaceTracers tracers;
	private final ExecutorService executorService = Executors.newFixedThreadPool(10);

	public ThriftAsyncKeyspaceImpl(AstyanaxConfiguration config) {
		this.config = config;
		this.connectionPool = config
			.getConnectionPoolFactory()
				.createConnectionPool(config,
					new ThriftAsyncConnectionFactoryImpl(config));
		this.partitioner = new RandomPartitioner();
		this.discovery = config
			.getNodeDiscoveryFactory().
				createNodeDiscovery(config, this, connectionPool);
		this.tracers = config.getKeyspaceTracers();
	}

	@Override
	public String getKeyspaceName() {
		return this.config.getKeyspaceName();
	}

	@Override
	public void start() {
		this.connectionPool.start();
		this.discovery.start();
	}

	@Override
	public void shutdown() {
		this.discovery.shutdown();
		this.connectionPool.shutdown();
	}

	@Override
	public <K,C> Query<K, C, ColumnList<C>> prepareGetRowQuery(
			final ColumnFamily<K, ?> columnFamily,
			final Serializer<C> columnSerializer,
			final K rowKey) {
		return new AbstractQueryImpl<K, C, ColumnList<C>>(null, config.getDefaultReadConsistencyLevel(), config.getSocketTimeout()) {
			@Override
			public Future<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<ColumnList<C>, Cassandra.AsyncClient.get_slice_call> operation =
                    new AbstractAsyncOperationImpl<ColumnList<C>, Cassandra.AsyncClient.get_slice_call>(
                        connectionPool,
                            executorService, config.getKeyspaceName(),
                        partitioner.getToken(columnFamily.getKeySerializer().toByteBuffer(rowKey)).token) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.get_slice(columnFamily.getKeySerializer().toByteBuffer(rowKey),
							    ThriftConverter.getColumnParent(columnFamily, path),
							    ThriftConverter.getPredicate(slice, columnSerializer),
							    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
							    this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public ColumnList<C> finishOperation(Cassandra.AsyncClient.get_slice_call response) throws ConnectionException {
                        List<ColumnOrSuperColumn> columnList;
						try {
							columnList = response.getResult();
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                        return new ThriftColumnOrSuperColumnListImpl<C>(columnList, columnSerializer);
                    }

                    @Override
                    public void trace(OperationResult<ColumnList<C>> result) {
                        tracers.incRowQuery(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
                    }
                };

                return AsyncFuture.make(operation);
            }
		};
	}

	@Override
	public <K,C> Query<K, C, Rows<K, C>> prepareGetMultiRowQuery(
			final ColumnFamily<K, ?> columnFamily,
			final Serializer<C> columnSerializer,
			final KeySlice<K> keys) {
        Preconditions.checkArgument(columnFamily != null, "CF must not be null");
        Preconditions.checkArgument(keys != null, "Keys must not be null");

		return new AbstractQueryImpl<K, C, Rows<K, C>> (null, config.getDefaultReadConsistencyLevel(), config.getSocketTimeout()) {
			@Override
			public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Rows<K, C>, ? extends TAsyncMethodCall>    operation;
                if (keys.getKeys() != null) {
                    operation = new AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.multiget_slice_call>(
                        connectionPool,
                        executorService,
                        config.getKeyspaceName()
                    ) {
                        @Override
                        public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                            try {
                                // Map of row key to Slice or Super slice
                                client.multiget_slice(
                                    columnFamily.getKeySerializer().toBytesList(keys.getKeys()),
                                    ThriftConverter.getColumnParent(columnFamily, path),
                                    ThriftConverter.getPredicate(slice, columnSerializer),
                                    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
                                    this);
                            }
                            catch (Exception e) {
                                throw ThriftConverter.ToConnectionPoolException(e);
                            }
                        }

                        @Override
                        public Rows<K, C> finishOperation(Cassandra.AsyncClient.multiget_slice_call response) throws ConnectionException {
                            Map<ByteBuffer, List<ColumnOrSuperColumn>> cfmap;
							try {
								cfmap = response.getResult();
							} catch (Exception e) {
								throw ThriftConverter.ToConnectionPoolException(e);
							}
                            if (cfmap == null) {
                                return new EmptyRowsImpl<K,C>();
                            }
                            else {
                                return new ThriftRowsListImpl<K, C>(cfmap, columnFamily.getKeySerializer(), columnSerializer);
                            }
                        }

                        @Override
                        public void trace(OperationResult<Rows<K, C>> result) {
                            tracers.incMultiRowQuery(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
                        }
                    };
                }
                else {
                    operation = new AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.get_range_slices_call>(
                        connectionPool,
                            executorService, config.getKeyspaceName()
                    ) {
                        @Override
                        public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                            try {
								// This is sorted list
								// Same call for standard and super columns via the ColumnParent
								KeyRange range = new KeyRange();
								if (keys.getStartKey() != null)
									range.setStart_key(columnFamily.getKeySerializer().toByteBuffer(keys.getStartKey()));
								if (keys.getEndKey() != null)
									range.setEnd_key(columnFamily.getKeySerializer().toByteBuffer(keys.getEndKey()));
								range.setCount(keys.getLimit());
								range.setStart_token(keys.getStartToken());
								range.setEnd_token(keys.getEndToken());

								client.get_range_slices(
                                    ThriftConverter.getColumnParent(columnFamily, path),
                                    ThriftConverter.getPredicate(slice, columnSerializer),
                                    range,
                                    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
                                    this);
                            }
                            catch (Exception e) {
                                throw ThriftConverter.ToConnectionPoolException(e);
                            }
                        }

                        @Override
                        public Rows<K, C> finishOperation(Cassandra.AsyncClient.get_range_slices_call response) throws ConnectionException {
                            List<org.apache.cassandra.thrift.KeySlice> keySlices;
							try {
								keySlices = response.getResult();
							} catch (Exception e) {
								throw ThriftConverter.ToConnectionPoolException(e);
							}
                            if (keySlices == null || keySlices.isEmpty()) {
                                return new EmptyRowsImpl<K,C>();
                            }
                            else {
                                return new ThriftRowsSliceImpl<K, C>(keySlices, columnFamily.getKeySerializer(), columnSerializer);
                            }
                        }

                        @Override
                        public void trace(OperationResult<Rows<K, C>> result) {
                            tracers.incMultiRowQuery(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
                        }
                    };
                }

                return AsyncFuture.make(operation);
			}
		};
	}

	@Override
	public <K, C> Query<K, C, Column<C>> prepareGetColumnQuery(
			final ColumnFamily<K, ?> columnFamily, final K key, ColumnPath<C> path) {

        Preconditions.checkArgument(columnFamily != null, "ColumnFamily must not be null");
        Preconditions.checkArgument(key != null, "Key must not be null");
        Preconditions.checkArgument(path != null, "Path must not be null");

		return new AbstractQueryImpl<K, C, Column<C>> (path, config.getDefaultReadConsistencyLevel(), config.getSocketTimeout()) {
			@Override
			public Future<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
				AbstractAsyncOperationImpl<Column<C>, Cassandra.AsyncClient.get_call> operation = new AbstractAsyncOperationImpl<Column<C>, Cassandra.AsyncClient.get_call>(
                    connectionPool,
                        executorService, config.getKeyspaceName(),
                    partitioner.getToken(columnFamily.getKeySerializer().toByteBuffer(key)).token
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.get(columnFamily.getKeySerializer().toByteBuffer(key),
							    ThriftConverter.getColumnPath(columnFamily, path),
							    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
							    this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public Column<C> finishOperation(Cassandra.AsyncClient.get_call response) throws ConnectionException {
                        ColumnOrSuperColumn column;
						try {
							column = response.getResult();
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                        if (column.isSetColumn()) {
                            org.apache.cassandra.thrift.Column c = column.getColumn();
                            return new ThriftColumnImpl<C>(path.getSerializer().fromBytes(c.getName()), c.getValue());
                        }
                        else if (column.isSetSuper_column()) {
                            SuperColumn sc = column.getSuper_column();
                            return new ThriftSuperColumnImpl<C>(path.getSerializer().fromBytes(sc.getName()), sc.getColumns());
                        }
                        else if (column.isSetCounter_column()) {
                            org.apache.cassandra.thrift.CounterColumn c = column.getCounter_column();
                            return new ThriftCounterColumnImpl<C>(path.getSerializer().fromBytes(c.getName()), c.getValue());
                        }
                        else if (column.isSetCounter_super_column()) {
                            CounterSuperColumn sc = column.getCounter_super_column();
                            return new ThriftCounterSuperColumnImpl<C>(path.getSerializer().fromBytes(sc.getName()), sc.getColumns());
                        }
                        else {
                            throw new RuntimeException("Unknown column type in response");
                        }
                    }

                    @Override
                    public void trace(OperationResult<Column<C>> result) {
                        tracers.incRowQuery(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
                    }
                };
                return AsyncFuture.make(operation);
			}
		};
	}

	@Override
	public MutationBatch prepareMutationBatch() {
		return new AbstractThriftMutationBatchImpl(config.getClock(), config.getDefaultWriteConsistencyLevel(), config.getSocketTimeout()) {
			@Override
			public Future<OperationResult<Void>> executeAsync()
					throws ConnectionException {
                BigInteger key = (getMutationMap().size() == 1) ?
                    partitioner.getToken(getMutationMap().keySet().iterator().next()).token : null;
				AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.batch_mutate_call> operation = new AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.batch_mutate_call>(
                    connectionPool,
                        executorService, config.getKeyspaceName(),
                    key
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.batch_mutate(getMutationMap(), ThriftConverter.ToThriftConsistencyLevel(consistencyLevel), this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public Void finishOperation(Cassandra.AsyncClient.batch_mutate_call response) throws ConnectionException {
                        discardMutations();
                        return null;
                    }

                    @Override
                    public void trace(OperationResult<Void> result) {
                        tracers.incMutation(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
                    }
                };
                return AsyncFuture.make(operation);
            }
		};
	}

	@Override
	public List<TokenRange> describeRing() throws ConnectionException {
        AbstractAsyncOperationImpl<List<TokenRange>, Cassandra.AsyncClient.describe_ring_call> operation = new AbstractAsyncOperationImpl<List<TokenRange>, Cassandra.AsyncClient.describe_ring_call>(
            connectionPool,
                executorService, config.getKeyspaceName()
        ) {
            @Override
            public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                try {
					client.describe_ring(getKeyspaceName(), this);
				} catch (TException e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
            }

            @Override
            public List<TokenRange> finishOperation(Cassandra.AsyncClient.describe_ring_call response) throws ConnectionException {
                List<org.apache.cassandra.thrift.TokenRange> tokenRanges;
				try {
					tokenRanges = response.getResult();
				} catch (Exception e) {
					throw ThriftConverter.ToConnectionPoolException(e);
				}
                return Lists.transform(tokenRanges,
                    new Function<org.apache.cassandra.thrift.TokenRange, TokenRange>() {
                        @Override
                        public TokenRange apply(
                                org.apache.cassandra.thrift.TokenRange tr) {
                            return new TokenRangeImpl(tr.getStart_token(), tr.getEnd_token(), tr.getEndpoints());
                        }

                    });
            }

            @Override
            public void trace(OperationResult<List<TokenRange>> result) {
                tracers.incMutation(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
            }
        };
        try {
            return AsyncFuture.make(operation).get().getResult();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnknownException(e);
        }
        catch (ExecutionException e) {
            throw ThriftConverter.ToConnectionPoolException(e);
        }
    }

	@Override
	public <K, C> CounterMutation<K, C> prepareCounterMutation(
			final ColumnFamily<K, C> columnFamily, final K rowKey, final ColumnPath<C> path,
			final long amount) {
		return new AbstractCounterMutationImpl<K,C>(config.getDefaultWriteConsistencyLevel(), config.getSocketTimeout()) {
            @Override
            public Future<OperationResult<Void>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.add_call> operation = new AbstractAsyncOperationImpl<Void, Cassandra.AsyncClient.add_call>(
                    connectionPool,
                        executorService, config.getKeyspaceName()
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        CounterColumn column = new CounterColumn();
                        column.setValue(amount);
                        column.setName(path.getLast());

                        try {
							client.add(columnFamily.getKeySerializer().toByteBuffer(rowKey),
							    ThriftConverter.getColumnParent(columnFamily, path),
							    column,
							    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
							    this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public Void finishOperation(Cassandra.AsyncClient.add_call response) throws ConnectionException {
                        return null;
                    }

                    @Override
                    public void trace(OperationResult<Void> result) {
                        tracers.incMutation(ThriftAsyncKeyspaceImpl.this, result.getHost(), result.getLatency());
                    }
                };
                return AsyncFuture.make(operation);
            }
        };
	}

	@Override
	public <K, C> ColumnFamilyQuery<K,C> prepareQuery(ColumnFamily<K, C> cf) {
		return new ThriftAsyncColumnFamilyQueryImpl<K, C>(connectionPool, config.getKeyspaceName(), config.getDefaultReadConsistencyLevel(), cf);
	}

	@Override
	public <K, C> ColumnMutation prepareColumnMutation(
			ColumnFamily<K, C> columnFamily, K rowKey, C column) {
		return new AbstractThriftColumnMutationImpl(
				columnFamily.getKeySerializer().toByteBuffer(rowKey),
				columnFamily.getColumnSerializer().toByteBuffer(column),
				config.getClock(),
				config.getDefaultReadConsistencyLevel(),
				config.getDefaultWriteConsistencyLevel()) {

			@Override
			public Execution<Void> incrementCounterColumn(long amount) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Execution<Void> deleteColumn() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Execution<Void> insertValue(ByteBuffer value,
					Integer ttl) {
				// TODO Auto-generated method stub
				return null;
			}
		
		};
	}
}
