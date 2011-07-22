package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ExecutionHelper;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.*;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyRowsImpl;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ThriftAsyncColumnFamilyQueryImpl<K, C> implements ColumnFamilyQuery<K, C> {
    private final ConnectionPool<Cassandra.AsyncClient> connectionPool;
    private final String keyspaceName;
    private final ColumnFamily<K,C> columnFamily;
    private ConsistencyLevel consistencyLevel;

    public ThriftAsyncColumnFamilyQueryImpl(ConnectionPool<Cassandra.AsyncClient> connectionPool, String keyspaceName, ConsistencyLevel consistencyLevel, ColumnFamily<K, C> columnFamily) {
        this.connectionPool = connectionPool;
        this.keyspaceName = keyspaceName;
        this.consistencyLevel = consistencyLevel;
        this.columnFamily = columnFamily;
    }

    @Override
    public RowQuery<K, C> getKey(final K rowKey) {
		return new AbstractRowQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            @Override
            public ColumnQuery<C> getColumn(final C column) {
                return new ColumnQuery<C>() {
                    @Override
                    public OperationResult<Column<C>> execute() throws ConnectionException {
                        return ExecutionHelper.blockingExecute(this);
                    }

                    @Override
                    public Future<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
                        AbstractAsyncOperationImpl<Column<C>, Cassandra.AsyncClient.get_call> operation = new AbstractAsyncOperationImpl<Column<C>, Cassandra.AsyncClient.get_call>(
                                connectionPool,
                                null, keyspaceName
                        ) {
                            @Override
                            public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                                try {
									client.get(
									    columnFamily.getKeySerializer().toByteBuffer(rowKey),
									    new org.apache.cassandra.thrift.ColumnPath()
									        .setColumn_family(columnFamily.getName())
									        .setColumn(columnFamily.getColumnSerializer().toByteBuffer(column)),
									    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
									    this);
								} catch (TException e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
                            }

                            @Override
                            public Column<C> finishOperation(Cassandra.AsyncClient.get_call response) throws ConnectionException {
                                ColumnOrSuperColumn cosc;
								try {
									cosc = response.getResult();
								} catch (Exception e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
								
                                if (cosc.isSetColumn()) {
                                    org.apache.cassandra.thrift.Column c = cosc.getColumn();
                                    return new ThriftColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(c.getName()), c.getValue());
                                }
                                else if (cosc.isSetSuper_column()) {
                                    // TODO: Super columns should be deprecated
                                    SuperColumn sc = cosc.getSuper_column();
                                    return new ThriftSuperColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(sc.getName()), sc.getColumns());
                                }
                                else if (cosc.isSetCounter_column()) {
                                    org.apache.cassandra.thrift.CounterColumn c = cosc.getCounter_column();
                                    return new ThriftCounterColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(c.getName()), c.getValue());
                                }
                                else if (cosc.isSetCounter_super_column()) {
                                    // TODO: Super columns should be deprecated
                                    CounterSuperColumn sc = cosc.getCounter_super_column();
                                    return new ThriftCounterSuperColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(sc.getName()), sc.getColumns());
                                }
                                else {
                                    throw new RuntimeException("Unknown column type in response");
                                }
                            }

                            @Override
                            public void trace(OperationResult<Column<C>> columnOperationResult) {
                            }
                        };
                        return AsyncFuture.make(operation);
                    }
                };
            }

            @Override
            public OperationResult<ColumnList<C>> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<ColumnList<C>, Cassandra.AsyncClient.get_slice_call> operation
                	= new AbstractAsyncOperationImpl<ColumnList<C>, Cassandra.AsyncClient.get_slice_call>(
                        connectionPool,
                        null, keyspaceName
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.get_slice(columnFamily.getKeySerializer().toByteBuffer(rowKey),
							    new ColumnParent().setColumn_family(columnFamily.getName()),
							    predicate,
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
                        return new ThriftColumnOrSuperColumnListImpl<C>(columnList, columnFamily.getColumnSerializer());
                    }

                    @Override
                    public void trace(OperationResult<ColumnList<C>> columnListOperationResult) {
                    }
                };
                return AsyncFuture.make(operation);
            }

			@Override
			public ColumnCountQuery getCount() {
				return new ColumnCountQuery() {

					@Override
					public OperationResult<Integer> execute() throws ConnectionException {
		                return ExecutionHelper.blockingExecute(this);
					}

					@Override
					public Future<OperationResult<Integer>> executeAsync() throws ConnectionException {
		                AbstractAsyncOperationImpl<Integer, Cassandra.AsyncClient.get_count_call> operation
		                	= new AbstractAsyncOperationImpl<Integer, Cassandra.AsyncClient.get_count_call>(
		                        connectionPool,
		                        null, keyspaceName) {
		                    @Override
		                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
		                        try {
									client.get_count(columnFamily.getKeySerializer().toByteBuffer(rowKey),
									    new ColumnParent().setColumn_family(columnFamily.getName()),
									    predicate,
									    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
									    this);
								} catch (TException e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
		                    }

		                    @Override
		                    public Integer finishOperation(Cassandra.AsyncClient.get_count_call response) throws ConnectionException {
								try {
									return response.getResult();
								} catch (Exception e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
		                    }

		                    @Override
		                    public void trace(OperationResult<Integer> result) {
		                    }
		                };
		                return AsyncFuture.make(operation);
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
    public RowSliceQuery<K, C> getKeyRange(final K startKey, final K endKey, final String startToken, final String endToken, final int count) {
        return new AbstractRowSliceQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.get_range_slices_call> operation = new AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.get_range_slices_call>(
                        connectionPool,
                        null, keyspaceName
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
						// This is a sorted list
						// Same call for standard and super columns via the ColumnParent
						KeyRange range = new KeyRange();
						if (startKey != null)
							range.setStart_key(columnFamily.getKeySerializer().toByteBuffer(startKey));
						if (endKey != null)
							range.setEnd_key(columnFamily.getKeySerializer().toByteBuffer(endKey));
						range.setCount(count);
						range.setStart_token(startToken);
						range.setEnd_token(endToken);

                        try {
							client.get_range_slices(
							    new ColumnParent().setColumn_family(columnFamily.getName()),
							    predicate,
							    range,
							    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
							    this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public Rows<K, C> finishOperation(Cassandra.AsyncClient.get_range_slices_call response) throws ConnectionException {
                        List<KeySlice> keySlices;
						try {
							keySlices = response.getResult();
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                        if (keySlices == null || keySlices.isEmpty()) {
                            return new EmptyRowsImpl<K,C>();
                        }
                        else {
                            return new ThriftRowsSliceImpl<K, C>(keySlices, columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
                        }
                    }

                    @Override
                    public void trace(OperationResult<Rows<K, C>> rowsOperationResult) {
                    }
                };
                return AsyncFuture.make(operation);
            }
        };
    }

    @Override
    public RowSliceQuery<K, C> getKeySlice(final K... keys) {
        return new AbstractRowSliceQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.multiget_slice_call> operation = new AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.multiget_slice_call>(
                        connectionPool,
                        null, keyspaceName
                ) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.multiget_slice(
							    columnFamily.getKeySerializer().toBytesList(Arrays.asList(keys)),
							    new ColumnParent().setColumn_family(columnFamily.getName()),
							    predicate,
							    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
							    this);
						} catch (TException e) {
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
                            return new ThriftRowsListImpl<K, C>(cfmap, columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
                        }
                    }

                    @Override
                    public void trace(OperationResult<Rows<K, C>> rowsOperationResult) {
                    }
                };
                return AsyncFuture.make(operation);
            }
        };
    }

    @Override
    public IndexQuery<K, C> searchWithIndex() {
        return new AbstractIndexQueryImpl<K, C>(columnFamily) {
            @Override
            public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
            }

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.get_indexed_slices_call> operation = new AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.get_indexed_slices_call>(
                        connectionPool,  null, keyspaceName) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.get_indexed_slices(
							    new ColumnParent().setColumn_family(columnFamily.getName()),
							    indexClause,
							    predicate,
							    ThriftConverter.ToThriftConsistencyLevel(consistencyLevel),
							    this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public Rows<K, C> finishOperation(Cassandra.AsyncClient.get_indexed_slices_call response) throws ConnectionException {
                        List<KeySlice> cfmap;
						try {
							cfmap = response.getResult();
	                        if (cfmap == null) {
	                            return new EmptyRowsImpl<K,C>();
	                        }
	                        else {
	                            return new ThriftRowsSliceImpl<K, C>(cfmap, columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
	                        }
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public void trace(OperationResult<Rows<K, C>> rowsOperationResult) {
                    }
                };
                return AsyncFuture.make(operation);
            }
        };
    }
    
	@Override
	public CqlQuery<K, C> withCql(final String cql) {
		return new CqlQuery<K,C>() {
			private boolean useCompression = false;
			 
			@Override
			public OperationResult<Rows<K, C>> execute() throws ConnectionException {
                return ExecutionHelper.blockingExecute(this);
			}
			
            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.execute_cql_query_call> operation = new AbstractAsyncOperationImpl<Rows<K, C>, Cassandra.AsyncClient.execute_cql_query_call>(
                        connectionPool, null, keyspaceName) {
                    @Override
                    public void startOperation(Cassandra.AsyncClient client) throws ConnectionException {
                        try {
							client.execute_cql_query(StringSerializer.get().toByteBuffer(cql), 
										useCompression ? Compression.GZIP : Compression.NONE, this);
						} catch (TException e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
                    }

                    @Override
                    public Rows<K, C> finishOperation(Cassandra.AsyncClient.execute_cql_query_call response) throws ConnectionException {
                        CqlResult cfmap;
						try {
							cfmap = response.getResult();
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
						switch (cfmap.getType()) {
						case INT:
							// TODO:
							break;
						case VOID:
							break;
						}
						return new ThriftCqlRowsImpl<K,C>(cfmap.getRows(), columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
                    }

                    @Override
                    public void trace(OperationResult<Rows<K, C>> rowsOperationResult) {
                    }
                };
                return AsyncFuture.make(operation);
            }
            
			@Override
			public CqlQuery<K, C> useCompression() {
				useCompression = true;
				return this;
			}
		};
	}}
