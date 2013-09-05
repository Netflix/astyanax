package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;

@SuppressWarnings("unchecked")
public class CqlRowSliceQueryImpl<K, C> implements RowSliceQuery<K, C> {

	private ChainedContext context; 
	private CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	
	public CqlRowSliceQueryImpl(ChainedContext ctx) {
		this.context = ctx;
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
	public RowSliceQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count) {
		// Cannot infer the actual column type C here. Use another class impl instead
		throw new NotImplementedException();
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {
		if (!(range instanceof CqlRangeImpl)) {
			throw new NotImplementedException();
		} else {
			this.columnSlice = new CqlColumnSlice<C>((CqlRangeImpl) range);
		}
		return this;
	}

	@Override
	public RowSliceColumnCountQuery<K> getColumnCounts() {
//		Query query = getQuery();
//		return new CqlRowSliceColumnCountQueryImpl<K>(context, query);
		throw new NotImplementedException();
	}
	

	


//	private OperationResult<Rows<K, C>> parseResponse(ResultSet rs) {
//		return new CqlOperationResultImpl<Rows<K, C>>(rs, new CqlRowListImpl<K, C>(rs.all()));
//	}
	
	
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
				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return queryGen.selectAllColumnsForRowKeys(rowSlice.getKeys());
				case COLUMN_COLLECTION:
					return queryGen.selectColumnSetForRowKeys(rowSlice.getKeys(), columnSlice.getColumns());
				case COLUMN_RANGE:
					return queryGen.selectColumnRangeForRowKeys(rowSlice.getKeys(), columnSlice);
				default:
					throw new NotImplementedException();
				}
			} else {
				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return queryGen.selectAllColumnsForRowRange(rowSlice.getRange());
				case COLUMN_COLLECTION:
					return queryGen.selectColumnSetForRowRange(rowSlice.getRange(), columnSlice.getColumns());
				case COLUMN_RANGE:
					return queryGen.selectColumnRangeForRowRange(rowSlice.getRange(), columnSlice);
				default:
					throw new NotImplementedException();
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

			public Query selectColumnSetForRowKeys(Collection<K> rowKeys, Collection<C> cols) {
					
				Object[] columns = cols.toArray(new Object[cols.size()]); 
				return QueryBuilder.select().all()
							.from(keyspace, cf.getName())
							.where(in("key", rowKeys.toArray()))
							.and(in("column1", columns));
			}

			public Query selectAllColumnsForRowRange(RowRange<K> range) {

				Select select = QueryBuilder.select().all()
						.from(keyspace, cf.getName());
				return addWhereClauseForRowKey("key", select, range);
			}

			public Query selectColumnSetForRowRange(RowRange<K> range, Collection<C> cols) {

				Object[] columns = cols.toArray(new Object[cols.size()]); 

				Select select = QueryBuilder.select().all().from(keyspace, cf.getName());
				Where where = addWhereClauseForRowKey("key", select, range);
				where.and(in("column1", columns));
				
				return where;
			}

			public Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice) {

				Select select = QueryBuilder.select().all()
						.from(keyspace, cf.getName());
				Where where = addWhereClauseForRowKey("key", select, range);			
				where = addWhereClauseForColumn(where, columnSlice);
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
				return addWhereClauseForRowKey(cf.getKeyAlias(), select, range);
			}

			@Override
			public Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice) {
				Select select = QueryBuilder.select()
						.from(keyspace, cf.getName());
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
		}
		
	}
	
	private static <K> Where addWhereClauseForRowKey(String keyAlias, Select select, RowRange<K> rowRange) {

		Where where = null;

		String tokenOfKey = "token(" + keyAlias + ")";

		if (rowRange.getStartKey() != null && rowRange.getEndKey() != null) {

			where = select.where(gte(tokenOfKey, rowRange.getStartKey()))
					.and(lte(tokenOfKey, rowRange.getEndKey()));

		} else if (rowRange.getStartKey() != null) {				
			where = select.where(gte(tokenOfKey, rowRange.getStartKey()));

		} else if (rowRange.getEndKey() != null) {
			where = select.where(lte(tokenOfKey, rowRange.getEndKey()));

		} else if (rowRange.getStartToken() != null && rowRange.getEndToken() != null) {

			where = select.where(gte(tokenOfKey, rowRange.getStartToken()))
					.and(lte(tokenOfKey, rowRange.getEndToken()));

		} else if (rowRange.getStartToken() != null) {
			where = select.where(gte(tokenOfKey, rowRange.getStartToken()));

		} else if (rowRange.getEndToken() != null) {
			where = select.where(lte(tokenOfKey, rowRange.getEndToken()));
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
	
	private static interface RowSliceQueryGenerator<K,C> {
		// QUERIES FOR ROW KEYS
		Query selectAllColumnsForRowKeys(Collection<K> rowKeys); 
		Query selectColumnSetForRowKeys(Collection<K> rowKeys, Collection<C> cols);
		Query selectColumnRangeForRowKeys(Collection<K> rowKeys, CqlColumnSlice<C> columnSlice);
		// QUERIES FOR ROW RANGES
		Query selectAllColumnsForRowRange(RowRange<K> range); 
		Query selectColumnSetForRowRange(RowRange<K> range, Collection<C> cols);
		Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice);
	}
}


