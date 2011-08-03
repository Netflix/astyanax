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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SuperColumn;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
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
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyRowsImpl;

/**
 * Implementation of all column family queries using the thrift API.
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class ThriftColumnFamilyQueryImpl<K,C> implements ColumnFamilyQuery<K,C> {
	private ConnectionPool<Cassandra.Client> connectionPool;
	private String keyspaceName;
	private ConsistencyLevel consistencyLevel;
	private ColumnFamily<K,C> columnFamily;
	
	public ThriftColumnFamilyQueryImpl(ConnectionPool<Cassandra.Client> cp, String keyspaceName, ColumnFamily<K, C> columnFamily, ConsistencyLevel consistencyLevel) {
		this.keyspaceName = keyspaceName;
		this.connectionPool = cp;
		this.consistencyLevel = consistencyLevel;
		this.columnFamily = columnFamily;
	}
	
	// Single ROW query
	@Override
	public RowQuery<K, C> getKey(final K rowKey) {
		return new AbstractRowQueryImpl<K, C>(columnFamily.getColumnSerializer()) {
			@Override
			public ColumnQuery<C> getColumn(final C column) {
				return new ColumnQuery<C>() {
					@Override
					public OperationResult<Column<C>> execute() throws ConnectionException {
						return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Column<C>>(keyspaceName) {
							@Override
							public Column<C> execute(Client client) throws ConnectionException, OperationException {
								try {
									ColumnOrSuperColumn cosc = client.get(
										columnFamily.getKeySerializer().toByteBuffer(rowKey), 
										new org.apache.cassandra.thrift.ColumnPath()
											.setColumn_family(columnFamily.getName())
											.setColumn(columnFamily.getColumnSerializer().toByteBuffer(column)),
										ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
									if (cosc.isSetColumn()) {
										org.apache.cassandra.thrift.Column c = cosc.getColumn();
										return new ThriftColumnImpl<C>(columnFamily.getColumnSerializer().fromBytes(c.getName()), c.getValue(), c.getTimestamp());
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
								catch (Exception e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
							}
						});
					}

					@Override
					public Future<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
		                throw new UnsupportedOperationException("Coming Soon");
					}
                };
			}

			@Override
			public OperationResult<ColumnList<C>> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<ColumnList<C>>(keyspaceName) {
					@Override
					public ColumnList<C> execute(Client client) throws ConnectionException, OperationException {
						try {
							List<ColumnOrSuperColumn> columnList =
								client.get_slice(columnFamily.getKeySerializer().toByteBuffer(rowKey), 
									new ColumnParent().setColumn_family(columnFamily.getName()),
									predicate,
									ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
							
							return new ThriftColumnOrSuperColumnListImpl<C>(columnList, columnFamily.getColumnSerializer());
						} 
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}

			@Override
			public ColumnCountQuery getCount() {
				return new ColumnCountQuery() {

					@Override
					public OperationResult<Integer> execute() throws ConnectionException {
						return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Integer>(keyspaceName) {
							@Override
							public Integer execute(Client client) throws ConnectionException, OperationException {
								try {
									return client.get_count(columnFamily.getKeySerializer().toByteBuffer(rowKey), 
											new ColumnParent().setColumn_family(columnFamily.getName()),
											predicate,
											ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
								} 
								catch (Exception e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
							}
						});
					}

					@Override
					public Future<OperationResult<Integer>> executeAsync() throws ConnectionException {
		                throw new UnsupportedOperationException("Coming Soon");
					}
					
				};
			}
			
            @Override
            public Future<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("Coming Soon");
            }
		};
	}

	@Override
	public RowSliceQuery<K, C> getKeyRange(final K startKey, final K endKey,
			final String startToken, final String endToken, final int count) {
		return new AbstractRowSliceQueryImpl<K,C>(columnFamily.getColumnSerializer()) {
			@Override
			public OperationResult<Rows<K, C>> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Rows<K, C>>(keyspaceName) {
					@Override
					public Rows<K, C> execute(Client client) throws ConnectionException,OperationException {
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
						
						List<org.apache.cassandra.thrift.KeySlice> keySlices;
						try {
							keySlices = client.get_range_slices(
									new ColumnParent().setColumn_family(columnFamily.getName()),
									predicate,
									range, 
									ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
							
							if (keySlices == null || keySlices.isEmpty()) {
								return new EmptyRowsImpl<K,C>();
							}
							else {
								return new ThriftRowsSliceImpl<K, C>(keySlices, columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
							}
						}
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("Coming Soon");
            }
		};
	}

	@Override
	public RowSliceQuery<K, C> getKeySlice(final K... keys) {
		return new AbstractRowSliceQueryImpl<K,C>(columnFamily.getColumnSerializer()) {
			@Override
			public OperationResult<Rows<K, C>> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Rows<K, C>>(keyspaceName) {
					@Override
					public Rows<K, C> execute(Client client) throws ConnectionException, OperationException {
						// Map of row key to Slice or Super slice
						Map<ByteBuffer, List<ColumnOrSuperColumn>> cfmap;
						try {
							cfmap = client.multiget_slice(
									columnFamily.getKeySerializer().toBytesList(Arrays.asList(keys)),
									new ColumnParent().setColumn_family(columnFamily.getName()),
									predicate,
									ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
							if (cfmap == null) {
								return new EmptyRowsImpl<K,C>();
							}
							else {
								return new ThriftRowsListImpl<K, C>(cfmap, columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
							}
						}
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("Coming Soon");
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
		return new AbstractIndexQueryImpl<K,C>(columnFamily) {
			@Override
			public OperationResult<Rows<K, C>> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Rows<K, C>>(keyspaceName) {

					@Override
					public Rows<K, C> execute(Client client)
							throws ConnectionException, OperationException {
						try {
							List<org.apache.cassandra.thrift.KeySlice> cfmap;
							cfmap = client.get_indexed_slices(
									new ColumnParent().setColumn_family(columnFamily.getName()),
									indexClause,
									predicate,
									ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
							
							if (cfmap == null) {
								return new EmptyRowsImpl<K,C>();
							}
							else {
								return new ThriftRowsSliceImpl<K, C>(cfmap, columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
							}
						}
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}

            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("Coming Soon");
            }
		};
	}

	@Override
	public CqlQuery<K, C> withCql(final String cql) {
		return new CqlQuery<K,C>() {
			private boolean useCompression = false;
			 
			@Override
			public OperationResult<Rows<K, C>> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Rows<K, C>>(keyspaceName) {
					@Override
					public Rows<K, C> execute(Client client) throws ConnectionException, OperationException {
						try {
							CqlResult result = client.execute_cql_query(StringSerializer.get().toByteBuffer(cql), 
									useCompression ? Compression.GZIP : Compression.NONE);
							switch (result.getType()) {
							case INT:
								// TODO:
								break;
							case VOID:
								break;
							}
							return new ThriftCqlRowsImpl<K,C>(result.getRows(), columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
						}
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				}); 
			}
            @Override
            public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("Coming Soon");
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
		return new AbstractThriftAllRowsQueryImpl<K,C>(columnFamily) {
			private AbstractThriftAllRowsQueryImpl<K,C> getThisQuery() {
				return this;
			}
			
			protected List<org.apache.cassandra.thrift.KeySlice> getNextBlock() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<List<org.apache.cassandra.thrift.KeySlice>>(keyspaceName) {
					@Override
					public List<org.apache.cassandra.thrift.KeySlice> execute(Client client)
							throws ConnectionException, OperationException {
						try {
							return client.get_range_slices(
									new ColumnParent().setColumn_family(columnFamily.getName()),
									predicate,
									range, 
									ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
						}
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				}).getResult();
			}

			@Override
			public OperationResult<Rows<K, C>> execute() throws ConnectionException {
				return connectionPool.executeWithFailover(new AbstractKeyspaceOperationImpl<Rows<K, C>>(keyspaceName) {
					@Override
					public Rows<K, C> execute(Client client)
							throws ConnectionException, OperationException {
						try {
							List<org.apache.cassandra.thrift.KeySlice> fistBlock = client.get_range_slices(
									new ColumnParent().setColumn_family(columnFamily.getName()),
									predicate,
									range, 
									ThriftConverter.ToThriftConsistencyLevel(consistencyLevel));
							return new ThriftAllRowsImpl<K,C>(getThisQuery(), columnFamily, fistBlock);
						}
						catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
				});
			}

			@Override
			public Future<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
                throw new UnsupportedOperationException("Coming Soon");
			}
		};
	}
}
