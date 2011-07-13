package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SuperColumn;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.IndexColumnExpression;
import com.netflix.astyanax.query.IndexOperationExpression;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.IndexValueExpression;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyRowsImpl;

/**
 * Implemention of all column family queries using the thrift API.
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
		return new RowQuery<K, C>() {
			private ColumnSlice<C> slice;
			
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
								catch (Exception e) {
									throw ThriftConverter.ToConnectionPoolException(e);
								}
							}
						});
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
									ThriftConverter.getPredicate(slice, columnFamily.getColumnSerializer()),
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
			public RowQuery<K, C> withColumnSlice(C... columns) {
				slice = new ColumnSlice<C>(Arrays.asList(columns));
				return this;
			}

			@Override
			public RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
				slice = new ColumnSlice<C>(startColumn, endColumn).setReversed(reversed).setLimit(count);
				return this;
			}
		};
	}

	@Override
	public RowSliceQuery<K, C> getKeyRange(final K startKey, final K endKey,
			final String startToken, final String endToken, final int count) {
		return new RowSliceQuery<K,C>() {
			private ColumnSlice<C> slice;

			@Override
			public RowSliceQuery<K, C> withColumnSlice(C... columns) {
				slice = new ColumnSlice<C>(Arrays.asList(columns));
				return this;
			}

			@Override
			public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
				slice = new ColumnSlice<C>(startColumn, endColumn).setReversed(reversed).setLimit(count);
				return this;
			}

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
									ThriftConverter.getPredicate(slice, columnFamily.getColumnSerializer()),
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
		};
	}

	@Override
	public RowSliceQuery<K, C> getKeySlice(final K... keys) {
		return new RowSliceQuery<K,C>() {
			private ColumnSlice<C> slice;

			@Override
			public RowSliceQuery<K, C> withColumnSlice(C... columns) {
				slice = new ColumnSlice<C>(Arrays.asList(columns));
				return this;
			}

			@Override
			public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
				slice = new ColumnSlice<C>(startColumn, endColumn).setReversed(reversed).setLimit(count);
				return this;
			}

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
									ThriftConverter.getPredicate(slice, columnFamily.getColumnSerializer()),
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
		};
	}


	@Override
	public ColumnFamilyQuery<K, C> setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	static interface IndexExpression<K,C> extends 
		IndexColumnExpression<K,C>, 
		IndexOperationExpression<K,C>, 
		IndexValueExpression<K,C>
	{
		
	}
	
	@Override	
	public IndexQuery<K, C> searchWithIndex() {
		return new IndexQuery<K,C>() {
			
			private final org.apache.cassandra.thrift.IndexClause indexClause =
				new org.apache.cassandra.thrift.IndexClause();

			private ColumnSlice<C> slice;
			
			@Override
			public IndexQuery<K, C> withColumnSlice(C... columns) {
				slice = new ColumnSlice<C>(Arrays.asList(columns));
				return this;
			}

			@Override
			public IndexQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
				slice = new ColumnSlice<C>(startColumn, endColumn).setReversed(reversed).setLimit(count);
				return this;
			}
			
			@Override
			public IndexQuery<K, C> setLimit(int count) {
				indexClause.setCount(count);
				return this;
			}

			@Override
			public IndexQuery<K, C> setStartKey(K key) {
				indexClause.setStart_key(columnFamily.getKeySerializer().toByteBuffer(key));
				return this;
			}
		
			private IndexQuery<K,C> getThisQuery() {
				return this;
			}
			
			@Override
			public IndexColumnExpression<K, C> addExpression() {
				return new IndexExpression<K,C>() {
					private final org.apache.cassandra.thrift.IndexExpression
						internalExpression = new org.apache.cassandra.thrift.IndexExpression();

					@Override
					public IndexOperationExpression<K, C> whereColumn(C columnName) {
						internalExpression.setColumn_name(columnFamily.getColumnSerializer().toBytes(columnName));
						return this;
					}

					@Override
					public IndexValueExpression<K, C> equals() {
						internalExpression.setOp(IndexOperator.EQ);
						return this;
					}

					@Override
					public IndexValueExpression<K, C> greaterThan() {
						internalExpression.setOp(IndexOperator.GT);
						return this;
					}

					@Override
					public IndexValueExpression<K, C> lessThan() {
						internalExpression.setOp(IndexOperator.LT);
						return this;
					}

					@Override
					public IndexValueExpression<K, C> greaterThanEquals() {
						internalExpression.setOp(IndexOperator.GTE);
						return this;
					}

					@Override
					public IndexValueExpression<K, C> lessThanEquals() {
						internalExpression.setOp(IndexOperator.LTE);
						return this;
					}

					@Override
					public IndexQuery<K, C> value(String value) {
						internalExpression.setValue(StringSerializer.get().toBytes(value));
						indexClause.addToExpressions(internalExpression);
						return getThisQuery();
					}

					@Override
					public IndexQuery<K, C> value(long value) {
						internalExpression.setValue(LongSerializer.get().toBytes(value));
						indexClause.addToExpressions(internalExpression);
						return getThisQuery();
					}

					@Override
					public IndexQuery<K, C> value(int value) {
						internalExpression.setValue(IntegerSerializer.get().toBytes(value));
						indexClause.addToExpressions(internalExpression);
						return getThisQuery();
					}

					@Override
					public IndexQuery<K, C> value(boolean value) {
						internalExpression.setValue(BooleanSerializer.get().toBytes(value));
						indexClause.addToExpressions(internalExpression);
						return getThisQuery();
					}
				};
			}

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
									ThriftConverter.getPredicate(slice, columnFamily.getColumnSerializer()),
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
		};
	}
}
