package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.RowQuery;

public class CqlRowQueryImpl<K, C> implements RowQuery<K, C> {

	private ChainedContext context; 
	private CqlColumnSlice<C> columnSlice;

	public CqlRowQueryImpl(ChainedContext ctx) {
		this.context = ctx;
	}

	@Override
	public OperationResult<ColumnList<C>> execute() throws ConnectionException {
		return new InternalRowQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
		return new InternalRowQueryExecutionImpl().executeAsync();
	}

	@Override
	public ColumnQuery<C> getColumn(C column) {
		return new CqlColumnQueryImpl<C>(context.clone().add(column));
	}

	@Override
	public RowQuery<K, C> withColumnSlice(Collection<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnSlice(C... columns) {
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public RowQuery<K, C> withColumnSlice(ColumnSlice<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
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
	public RowQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count) {
		// Cannot infer the actual column type C here. Use another class impl instead
		throw new NotImplementedException();
	}

	@Override
	public RowQuery<K, C> withColumnRange(ByteBufferRange range) {
		if (!(range instanceof CqlRangeImpl)) {
			throw new NotImplementedException();
		} else {
			this.columnSlice = new CqlColumnSlice<C>((CqlRangeImpl) range);
		}
		return this;
	}

	@Override
	@Deprecated
	public RowQuery<K, C> setIsPaginating() {
		throw new NotImplementedException();
	}

	@Override
	public RowQuery<K, C> autoPaginate(boolean enabled) {
		throw new NotImplementedException();
	}

	@Override
	public RowCopier<K, C> copyTo(ColumnFamily<K, C> columnFamily, K rowKey) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnCountQuery getCount() {
		return new CqlColumnCountQueryImpl(this.context.clone());
	}

	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<ColumnList<C>> {

		public InternalRowQueryExecutionImpl() {
			super(context);
		}

		@Override
		public Query getQuery() {

			String keyspace = context.getNext(String.class);
			ColumnFamily<?, ?> cf = context.getNext(ColumnFamily.class);
			Object rowKey = context.getNext(Object.class); 

			Query query = null;

			Collection<C> columns = columnSlice.getColumns();

			if (columns != null) {

				Selection selection = QueryBuilder.select();

				for (C column : columns) {
					selection.column(String.valueOf(column));
				}

				query = selection.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));

			} else {

				Where stmt = QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));

				if (columnSlice.getStartColumn() != null) {
					stmt.and(gte(columnSlice.getColumnName(), columnSlice.getStartColumn()));
				}
				if (columnSlice.getEndColumn() != null) {
					stmt.and(lte(columnSlice.getColumnName(), columnSlice.getEndColumn()));
				}

				if (columnSlice.getReversed()) {
					stmt.desc(columnSlice.getColumnName());
				}

				if (columnSlice.getLimit() != -1) {
					stmt.limit(columnSlice.getLimit());
				}

				query = stmt;
			}
			return query;
		}

		@Override
		public ColumnList<C> parseResultSet(ResultSet rs) {

			List<Row> rows = rs.all(); 

			if (columnSlice.getColumns() != null) {
				if (rows.size() > 1) {
					throw new RuntimeException("Multiple rows for query - use RowSliceQuery instead");
				} else {
					return new CqlColumnListImpl<C>(rows.get(0));
				}
			} else {
				return new CqlColumnListImpl<C>(rows);
			}
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}
	}
}
