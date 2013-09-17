package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.reads.CqlRowSlice.RowRange;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;

@SuppressWarnings("unchecked")
public class CqlRowSliceQueryImpl<K, C> implements RowSliceQuery<K, C> {

	private ChainedContext context; 
	private ColumnFamily<K,C> cf;
	private CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	private CompositeByteBufferRange compositeRange = null;
	
	public CqlRowSliceQueryImpl(ChainedContext ctx) {
		this.context = ctx.rewindForRead();
		this.cf = this.context.skip().skip().getNext(ColumnFamily.class);
	}
	
	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		return new InternalRowQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
		return new InternalRowQueryExecutionImpl().executeAsync();
	}
	
	@Override
	public RowSliceQuery<K, C> withColumnSlice(C... columns) {
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(Collection<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(ColumnSlice<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		this.columnSlice = new CqlColumnSlice<C>(new CqlRangeBuilder<C>()
				.setColumn("column1")
				.setStart(startColumn)
				.setEnd(endColumn)
				.setReversed(reversed)
				.setLimit(count)
				.build());
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int limit) {
		Serializer<C> colSerializer = cf.getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		// TODO this is used for composite columns. Need to fix this.
		return this.withColumnRange(start, end, reversed, limit);
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {
		
		if (range instanceof CompositeByteBufferRange) {
			this.compositeRange = (CompositeByteBufferRange) range;
			
		} else if (range instanceof CompositeRangeBuilder) {
			this.compositeRange = ((CompositeRangeBuilder)range).build();
			
		} else if (range instanceof CqlRangeImpl) {
			this.columnSlice.setCqlRange((CqlRangeImpl<C>) range);
		} else {
			return this.withColumnRange(range.getStart(), range.getEnd(), range.isReversed(), range.getLimit());
		}
		return this;
	}

	@Override
	public RowSliceColumnCountQuery<K> getColumnCounts() {
		Query query = new InternalRowQueryExecutionImpl().getQuery();
		return new CqlRowSliceColumnCountQueryImpl<K>(context, query);
	}
	
	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<Rows<K, C>> {

		private final CqlRowSlice<K> rowSlice;

		public InternalRowQueryExecutionImpl() {
			super(context);
			this.rowSlice = context.getNext(CqlRowSlice.class); 
		}

		@Override
		public Query getQuery() {

			RowSliceQueryGenerator<K,C> queryGen = (CqlFamilyFactory.OldStyleThriftMode()) ? new OldStyle<K,C>() : new NewStyle<K,C>();

			if (rowSlice.isCollectionQuery()) {
				
				if (compositeRange != null) {
					return queryGen.selectCompositeColumnRangeForRowKeys(rowSlice.getKeys(), compositeRange);
				}
				
				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return queryGen.selectAllColumnsForRowKeys(rowSlice.getKeys());
				case COLUMN_COLLECTION:
					return queryGen.selectColumnSetForRowKeys(rowSlice.getKeys(), columnSlice.getColumns());
				case COLUMN_RANGE:
					return queryGen.selectColumnRangeForRowKeys(rowSlice.getKeys(), columnSlice);
				default:
					throw new IllegalStateException();
				}
			} else {
				
				if (compositeRange != null) {
					return queryGen.selectCompositeColumnRangeForRowRange(rowSlice.getRange(), compositeRange);
				}

				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return queryGen.selectAllColumnsForRowRange(rowSlice.getRange());
				case COLUMN_COLLECTION:
					return queryGen.selectColumnSetForRowRange(rowSlice.getRange(), columnSlice.getColumns());
				case COLUMN_RANGE:
					return queryGen.selectColumnRangeForRowRange(rowSlice.getRange(), columnSlice);
				default:
					throw new IllegalStateException();
				}
			}
		}

		@Override
		public Rows<K, C> parseResultSet(ResultSet rs) {

			List<com.datastax.driver.core.Row> rows = rs.all(); 

			boolean oldStyle = CqlFamilyFactory.OldStyleThriftMode();
			return new CqlRowListImpl<K, C>(rows, cf, oldStyle);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}
		
		
		/** OLD STYLE QUERIES */ 
		
		private class OldStyle<K,C> implements RowSliceQueryGenerator<K,C> {
			
			@Override
			public Query selectAllColumnsForRowKeys(Collection<K> rowKeys) {
				return QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(in("key", rowKeys.toArray()));
			}
			
			@Override
			public Query selectColumnRangeForRowKeys(Collection<K> rowKeys, CqlColumnSlice<C> columnSlice) {
				Where where = QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(in("key", rowKeys.toArray()));
				where = addWhereClauseForColumn(where, columnSlice);
				return where;
			}

			@Override
			public Query selectColumnSetForRowKeys(Collection<K> rowKeys, Collection<C> cols) {
					
				Object[] columns = cols.toArray(new Object[cols.size()]); 
				return QueryBuilder.select().all()
							.from(keyspace, cf.getName())
							.where(in("key", rowKeys.toArray()))
							.and(in("column1", columns));
			}

			@Override
			public Query selectCompositeColumnRangeForRowKeys(Collection<K> rowKeys, CompositeByteBufferRange compositeRange) {
				
				Where stmt = QueryBuilder.select().all()
						.from(keyspace, cf.getName())
							.where(in("key", rowKeys.toArray()));

				stmt = addWhereClauseForCompositeColumnRange(stmt, compositeRange);
				return stmt;
			}

			@Override
			public Query selectAllColumnsForRowRange(RowRange<K> range) {

				Select select = QueryBuilder.select().all()
						.from(keyspace, cf.getName());
				return addWhereClauseForRowKey("key", select, range);
			}

			public Query selectColumnSetForRowRange(RowRange<K> range, Collection<C> cols) {

				Object[] columns = cols.toArray(new Object[cols.size()]); 

				Select select = QueryBuilder.select().all().from(keyspace, cf.getName());
				if (columns != null && columns.length > 0) {
					select.allowFiltering();
				}
				Where where = addWhereClauseForRowKey("key", select, range);
				where.and(in("column1", columns));
				
				return where;
			}

			public Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice) {

				Select select = QueryBuilder.select().all().from(keyspace, cf.getName());
				if (columnSlice != null && columnSlice.isRangeQuery()) {
					select.allowFiltering();
				}

				Where where = addWhereClauseForRowKey("key", select, range);			
				where = addWhereClauseForColumn(where, columnSlice);
				return where;
			}

			@Override
			public Query selectCompositeColumnRangeForRowRange(RowRange<K> range, CompositeByteBufferRange compositeRange) {

				Select select = QueryBuilder.select().all().from(keyspace, cf.getName());
				if (compositeRange != null) {
					select.allowFiltering();
				}

				Where where = addWhereClauseForRowKey("key", select, range);	
				where = addWhereClauseForCompositeColumnRange(where, compositeRange);
				return where;
			}
		}
		
		/** NEW STYLE QUERIES */
		private class NewStyle<K,C> implements RowSliceQueryGenerator<K,C> {

			@Override
			public Query selectAllColumnsForRowKeys(Collection<K> rowKeys) {
				return QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(in(cf.getKeyAlias(), rowKeys.toArray()));
			}

			@Override
			public Query selectColumnSetForRowKeys(Collection<K> rowKeys, Collection<C> cols) {

				return QueryBuilder.select(getColumnsArray(cols))
							.from(keyspace, cf.getName())
							.where(in(cf.getKeyAlias(), rowKeys.toArray()));
			}

			@Override
			public Query selectColumnRangeForRowKeys(Collection<K> rowKeys, CqlColumnSlice<C> columnSlice) {

				Where where =  QueryBuilder.select()
								.from(keyspace, cf.getName())
								.where(in(cf.getKeyAlias(), rowKeys.toArray()));
				return addWhereClauseForColumnRange(where, columnSlice);
			}

			@Override
			public Query selectAllColumnsForRowRange(RowRange<K> range) {

				Select select = QueryBuilder.select().all()
						.from(keyspace, cf.getName());
				return addWhereClauseForRowKey(cf.getKeyAlias(), select, range);
			}

			@Override
			public Query selectColumnSetForRowRange(RowRange<K> range, Collection<C> cols) {

				Select select = QueryBuilder.select(getColumnsArray(cols))
											.from(keyspace, cf.getName());
				if (cols != null && cols.size() > 0) {
					select.allowFiltering();
				}
				return addWhereClauseForRowKey(cf.getKeyAlias(), select, range);
			}

			@Override
			public Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice) {
				Select select = QueryBuilder.select()
						.from(keyspace, cf.getName());
				if (columnSlice != null && columnSlice.isRangeQuery()) {
					select.allowFiltering();
				}

				Where where = addWhereClauseForRowKey(cf.getKeyAlias(), select, range);
				where = addWhereClauseForColumnRange(where, columnSlice);
				return where;
			}
			
			private String[] getColumnsArray(Collection<C> cols) {
				int index=0;
				String[] columns = new String[cols.size()];
				for (C col : cols) {
					columns[index++] = String.valueOf(col);
				}
				return columns;
			}

			@Override
			public Query selectCompositeColumnRangeForRowKeys(Collection<K> rowKeys, CompositeByteBufferRange compositeRange) {
				throw new NotImplementedException();
			}

			@Override
			public Query selectCompositeColumnRangeForRowRange(RowRange<K> range, CompositeByteBufferRange compositeRange) {
				throw new NotImplementedException();
			}
		}
		
	}
	
	private static <K> Where addWhereClauseForRowKey(String keyAlias, Select select, RowRange<K> rowRange) {

		Where where = null;

		boolean keyIsPresent = false;
		boolean tokenIsPresent = false; 
		
		if (rowRange.getStartKey() != null || rowRange.getEndKey() != null) {
			keyIsPresent = true;
		}
		if (rowRange.getStartToken() != null || rowRange.getEndToken() != null) {
			tokenIsPresent = true;
		}
		
		if (keyIsPresent && tokenIsPresent) {
			throw new RuntimeException("Cannot provide both token and keys for range query");
		}
		
		if (keyIsPresent) {
			if (rowRange.getStartKey() != null && rowRange.getEndKey() != null) {

				where = select.where(gte(keyAlias, rowRange.getStartKey()))
						.and(lte(keyAlias, rowRange.getEndKey()));

			} else if (rowRange.getStartKey() != null) {				
				where = select.where(gte(keyAlias, rowRange.getStartKey()));

			} else if (rowRange.getEndKey() != null) {
				where = select.where(lte(keyAlias, rowRange.getEndKey()));
			}
			
		} else if (tokenIsPresent) {
			String tokenOfKey ="token(" + keyAlias + ")";

			BigInteger startToken = rowRange.getStartToken() != null ? new BigInteger(rowRange.getStartToken()) : null; 
			BigInteger endToken = rowRange.getEndToken() != null ? new BigInteger(rowRange.getEndToken()) : null; 
			
			if (startToken != null && endToken != null) {

				where = select.where(gte(tokenOfKey, startToken))
								.and(lte(tokenOfKey, endToken));

			} else if (startToken != null) {
				where = select.where(gte(tokenOfKey, startToken));

			} else if (endToken != null) {
				where = select.where(lte(tokenOfKey, endToken));
			}
			
		} else { 
			where = select.where();
		}

		if (rowRange.getCount() > 0) {
			where.limit(rowRange.getCount());
		}

		return where; 
	}

	private static <C> Where addWhereClauseForColumn(Where where, CqlColumnSlice<C> columnSlice) {

		if (!columnSlice.isRangeQuery()) {
			return where;
		}
		if (columnSlice.getStartColumn() != null) {
			where.and(gte("column1", columnSlice.getStartColumn()));
		}
		if (columnSlice.getEndColumn() != null) {
			where.and(lte("column1", columnSlice.getEndColumn()));
		}

		if (columnSlice.getReversed()) {
			where.orderBy(desc("column1"));
		}

		if (columnSlice.getLimit() != -1) {
			where.limit(columnSlice.getLimit());
		}

		return where;
	}
	
	private static <C> Where addWhereClauseForColumnRange(Where stmt, CqlColumnSlice<C> columnSlice) {

		if (columnSlice.getStartColumn() != null) {
			stmt.and(gte(columnSlice.getColumnName(), columnSlice.getStartColumn()));
		}
		
		if (columnSlice.getEndColumn() != null) {
			stmt.and(lte(columnSlice.getColumnName(), columnSlice.getEndColumn()));
		}

		if (columnSlice.getReversed()) {
			stmt.orderBy(desc(columnSlice.getColumnName()));
		}

		if (columnSlice.getLimit() != -1) {
			stmt.limit(columnSlice.getLimit());
		}
		
		return stmt;
	}
	
	private static Where addWhereClauseForCompositeColumnRange(Where stmt, CompositeByteBufferRange compositeRange) {

		List<RangeQueryRecord> records = compositeRange.getRecords();
		int componentIndex = 1; 

		for (RangeQueryRecord record : records) {

			String columnName = "column" + componentIndex;

			for (RangeQueryOp op : record.getOps()) {

				switch (op.getOperator()) {

				case EQUAL:
					stmt.and(eq(columnName, op.getValue()));
					break;
				case LESS_THAN :
					stmt.and(lt(columnName, op.getValue()));
					break;
				case LESS_THAN_EQUALS:
					stmt.and(lte(columnName, op.getValue()));
					break;
				case GREATER_THAN:
					stmt.and(gt(columnName, op.getValue()));
					break;
				case GREATER_THAN_EQUALS:
					stmt.and(gte(columnName, op.getValue()));
					break;
				default:
					throw new RuntimeException("Cannot recognize operator: " + op.getOperator().name());
				}; // end of switch stmt
			} // end of inner for for ops for each range query record
			componentIndex++;
		}
		return stmt;
	}

	private static interface RowSliceQueryGenerator<K,C> {
		// QUERIES FOR ROW KEYS
		Query selectAllColumnsForRowKeys(Collection<K> rowKeys); 
		Query selectColumnSetForRowKeys(Collection<K> rowKeys, Collection<C> cols);
		Query selectColumnRangeForRowKeys(Collection<K> rowKeys, CqlColumnSlice<C> columnSlice);
		Query selectCompositeColumnRangeForRowKeys(Collection<K> rowKeys, CompositeByteBufferRange compositeRange);
		// QUERIES FOR ROW RANGES
		Query selectAllColumnsForRowRange(RowRange<K> range); 
		Query selectColumnSetForRowRange(RowRange<K> range, Collection<C> cols);
		Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice);
		Query selectCompositeColumnRangeForRowRange(RowRange<K> range, CompositeByteBufferRange compositeRange);
	}
}


