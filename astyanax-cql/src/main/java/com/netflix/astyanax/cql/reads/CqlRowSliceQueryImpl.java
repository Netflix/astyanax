package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;

@SuppressWarnings("unchecked")
public class CqlRowSliceQueryImpl<K, C> implements RowSliceQuery<K, C> {

	private ChainedContext context; 
	private CqlColumnSlice<C> columnSlice;
	
	public CqlRowSliceQueryImpl(ChainedContext ctx) {
		this.context = ctx;
	}
	

	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		
		context.rewindForRead();
		Cluster cluster = context.getNext(Cluster.class);
		
		Query query = getQuery();
		ResultSet rs = cluster.connect().execute(query);
		return new CqlOperationResultImpl<Rows<K, C>>(rs, new CqlRowListImpl<K, C>(rs.all()));
	}
	
	private Query getQuery() {
		String keyspace = context.getNext(String.class);
		ColumnFamily<?, ?> cf = context.getNext(ColumnFamily.class);
		CqlRowSlice<K> rowSlice = context.getNext(CqlRowSlice.class); 
		
		String keyAlias = (cf.getKeyAlias() != null) ? cf.getKeyAlias() : "key";

		Selection selection = QueryBuilder.select();

		// Decide what columns to select, if any specified. 
		if (columnSlice != null && columnSlice.isColumnSelectQuery()) {

			Collection<C> columns = columnSlice.getColumns();
			if (columns != null ) {
				if (columns.size() == 0) {
					// select all the columns
					selection.all();
				} else {
					// Check if the key has already been selected, if not, then explicitly select it
					boolean keyAliasPresent = columns.contains(keyAlias);
					if (!keyAliasPresent) {
						selection.column(keyAlias);
					}

					for (C column : columns) {
						selection.column(String.valueOf(column));
					}
				}
			}
		}
		
		// Select FROM table
		Select select = selection.from(keyspace, cf.getName());
		
		// The WHERE clause which uses the primary key
		Where where = addWhereClause(keyAlias, select, rowSlice);
			
		// Check if we have a secondary key in the WHERE clause, in case of a composite primary key
		if (columnSlice != null && columnSlice.isRangeQuery()) {
			if (columnSlice.getStartColumn() != null) {
				where.and(gte(columnSlice.getColumnName(), columnSlice.getStartColumn()));
			}
			if (columnSlice.getEndColumn() != null) {
				where.and(lte(columnSlice.getColumnName(), columnSlice.getEndColumn()));
			}

			if (columnSlice.getReversed()) {
				where.desc(columnSlice.getColumnName());
			}
			
			if (columnSlice.getLimit() != -1) {
				where.limit(columnSlice.getLimit());
			}
		}
		
		return where;
	}

	
	private Where addWhereClause(String keyAlias, Select select, CqlRowSlice<K> rowSlice) {
		
		Collection<K> keys = rowSlice.getKeys(); 
		
		if (keys != null && keys.size() > 0) {
			return select.where(in(keyAlias, keys.toArray()));
			
		} else { 

			Where where = null;
			
			String tokenOfKey = "token(" + keyAlias + ")";
			
			if (rowSlice.getStartKey() != null && rowSlice.getEndKey() != null) {
			
				where = select.where(gte(tokenOfKey, rowSlice.getStartKey()))
				.and(lte(tokenOfKey, rowSlice.getEndKey()));
			
			} else if (rowSlice.getStartKey() != null) {				
				where = select.where(gte(tokenOfKey, rowSlice.getStartKey()));
			
			} else if (rowSlice.getEndKey() != null) {
				where = select.where(lte(tokenOfKey, rowSlice.getEndKey()));

			} else if (rowSlice.getStartToken() != null && rowSlice.getEndToken() != null) {

				where = select.where(gte(tokenOfKey, rowSlice.getStartToken()))
				.and(lte(tokenOfKey, rowSlice.getEndToken()));

			} else if (rowSlice.getStartToken() != null) {
				where = select.where(gte(tokenOfKey, rowSlice.getStartToken()));
			
			} else if (rowSlice.getEndToken() != null) {
				where = select.where(lte(tokenOfKey, rowSlice.getEndToken()));
			}

			if (where == null) {
				where = select.where();
			}
			
			if (rowSlice.getCount() > 0) {
				where.limit(rowSlice.getCount());
			}
			
			return where; 
		}
	}

	@Override
	public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
		throw new NotImplementedException();
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
		context.rewindForRead();
		Cluster cluster = context.getNext(Cluster.class);
		Query query = getQuery();
		return new CqlRowSliceColumnCountQueryImpl<K>(cluster, query);
	}

}
