package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.reads.model.CqlColumnListImpl;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.reads.model.CqlRangeImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;

/**
 * Impl for {@link RowQuery} that uses java driver. It manages all single row queries and also has support for pagination. 
 * All {@link ColumnQuery} and {@link ColumnCountQuery}(s) originate from this class. 
 * 
 * Note that the class acts more like a placeholder for the structure query context. The actual query construction 
 * is done by other classes like {@link CFRowQueryGen} and {@link CFColumnQueryGen}
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class CqlRowQueryImpl<K, C> implements RowQuery<K, C> {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<K,C> cfContext;

	private final Object rowKey;
	private final CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	private CompositeByteBufferRange compositeRange;
	private final PaginationContext paginationContext = new PaginationContext();

	public enum RowQueryType {
		AllColumns, ColumnSlice, ColumnRange, SingleColumn; 
	}
	
	private RowQueryType queryType = RowQueryType.AllColumns;  // The default
	private boolean useCaching = false;
	
	public CqlRowQueryImpl(KeyspaceContext ksCtx, CFQueryContext<K,C> cfCtx, K rKey, boolean useCaching) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowKey = cfCtx.checkRowKey(rKey);
		this.useCaching = useCaching;
	}

	@Override
	public OperationResult<ColumnList<C>> execute() throws ConnectionException {

		if (paginationContext.isPaginating()) {
			if (!paginationContext.isFirstPage()) {
				return new CqlOperationResultImpl<ColumnList<C>>(paginationContext.getResultSet(), paginationContext.getNextColumns());
			}
			// Note that if we are paginating, and if this is the first time / page, 
			// then we will just execute the query normally, and then init the pagination context
		}

		return new InternalRowQueryExecutionImpl(this).execute();
	}

	@Override
	public ListenableFuture<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
		return new InternalRowQueryExecutionImpl(this).executeAsync();
	}

	@Override
	public ColumnQuery<C> getColumn(C column) {
		queryType = RowQueryType.SingleColumn;
		return new CqlColumnQueryImpl<C>(ksContext, cfContext, rowKey, column, useCaching);
	}

	@Override
	public RowQuery<K, C> withColumnSlice(Collection<C> columns) {
		queryType = RowQueryType.ColumnSlice;
		this.columnSlice.setColumns(columns);
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnSlice(C... columns) {
		queryType = RowQueryType.ColumnSlice;
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public RowQuery<K, C> withColumnSlice(ColumnSlice<C> colSlice) {
		if (colSlice.getColumns() != null) {
			return withColumnSlice(colSlice.getColumns());
		} else {
			return withColumnRange(colSlice.getStartColumn(), colSlice.getEndColumn(), colSlice.getReversed(), colSlice.getLimit());
		}
	}

	@Override
	public RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		
		queryType = RowQueryType.ColumnRange;

		this.columnSlice.setCqlRange(new CqlRangeBuilder<C>()
				.setColumn("column1")
				.setStart(startColumn)
				.setEnd(endColumn)
				.setReversed(reversed)
				.setLimit(count)
				.build());
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int limit) {

		queryType = RowQueryType.ColumnRange;

		Serializer<C> colSerializer = cfContext.getColumnFamily().getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		return this.withColumnRange(start, end, reversed, limit);
	}

	@SuppressWarnings("unchecked")
	@Override
	public RowQuery<K, C> withColumnRange(ByteBufferRange range) {

		queryType = RowQueryType.ColumnRange;

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
	@Deprecated
	public RowQuery<K, C> setIsPaginating() {
		return autoPaginate(true);
	}

	@Override
	public RowQuery<K, C> autoPaginate(boolean enabled) {
		paginationContext.setPaginating(enabled);
		return this;
	}

	@Override
	public RowCopier<K, C> copyTo(ColumnFamily<K, C> columnFamily, K rowKey) {
		return new CqlRowCopier<K,C>(columnFamily, rowKey, this, ksContext);
	}

	@Override
	public ColumnCountQuery getCount() {
		return new CqlColumnCountQueryImpl(ksContext, cfContext, new InternalRowQueryExecutionImpl(this).getQuery());
	}

	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<ColumnList<C>> {

		private final CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();

		private final String[] allPkColumnNames = cfDef.getAllPkColNames();
		private final List<ColumnDefinition> regularCols = cfDef.getRegularColumnDefinitionList();

		private final CqlRowQueryImpl<?,?> rowQuery; 
		
		public InternalRowQueryExecutionImpl(CqlRowQueryImpl<?,?> rQuery) {
			super(ksContext, cfContext);
			this.rowQuery = rQuery;
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}

		@Override
		public Statement getQuery() {
			
			Statement stmt = cfDef.getRowQueryGenerator().getQueryStatement(rowQuery, useCaching);
			// Translate the column limit to the fetch size. This is useful for pagination
			if (paginationContext.isPaginating() && columnSlice.isRangeQuery()) {
//				if (columnSlice.getFetchSize() > 0) {
//					stmt.setFetchSize(columnSlice.getFetchSize() + 1);
//				}
			}
			return stmt;
		}

		@Override
		public ColumnList<C> parseResultSet(ResultSet resultSet) throws NotFoundException {

			// Use case when the schema is just a flat table. Note that there is no pagination support here.
			if (allPkColumnNames.length == 1 || regularCols.size() > 1) {
				List<Row> rows = resultSet.all(); 
				if (rows == null || rows.isEmpty()) {
					return new CqlColumnListImpl<C>();
				} else {
					return new CqlColumnListImpl<C>(rows.get(0), cf);
				}
			}
			
			// There is a clustering key for this schema. Check whether we are paginating for this row query
			if (paginationContext.isPaginating()) {
				
				paginationContext.init(resultSet, columnSlice.getFetchSize());
				return paginationContext.getNextColumns();
				
			} else {
				
				List<Row> rows = resultSet.all(); 
				if (rows == null || rows.isEmpty()) {
					return new CqlColumnListImpl<C>();
				} else {
					return new CqlColumnListImpl<C>(rows, cf);
				}
			}
			
			
//			List<Row> rows = resultSet.all(); 
//
//			if (rows == null || rows.isEmpty()) {
//				if (paginationContext.isPaginating()) {
//					paginationContext.lastPageConsumed = true;
//				}
//				return new CqlColumnListImpl<C>();
//			}
//			
//			if (allPkColumnNames.length == 1 || regularCols.size() > 1) {
//				CqlColumnListImpl<C> columnList = new CqlColumnListImpl<C>(rows.get(0), cf);
//				return columnList;
//			} else {
//				CqlColumnListImpl<C> columnList = new CqlColumnListImpl<C>(rows, cf);
//				paginationContext.trackLastColumn(columnList);
//				return columnList;
//			}
		}


	}
	
	
	private class PaginationContext {
		
		// How many rows to fetch at a time
		private int fetchSize = Integer.MAX_VALUE; 
		
		// Turn pagination ON/OFF
		private boolean paginate = false;
		// Indicate whether the first page has been consumed. 
		private boolean isFirstPage = true;
		// Track the result set
		private ResultSet resultSet = null;
		// State for all rows
		private Iterator<Row> rowIter = null;
		
		private PaginationContext() {
		}
		
		private void setPaginating(boolean condition) {
			paginate = condition;
		}
		
		private boolean isPaginating() {
			return paginate;
		}
		
		private boolean isFirstPage() {
			return isFirstPage;
		}
		
		private void firstPageConsumed() {
			isFirstPage = false;
		}

		private CqlColumnListImpl<C> getNextColumns() {
			
			try { 
				int count = 0;
				List<Row> rows = new ArrayList<Row>();
				while ((count < fetchSize) && rowIter.hasNext()) {
					rows.add(rowIter.next());
					count++;
				}
				return new CqlColumnListImpl<C>(rows, cfContext.getColumnFamily());
			} finally {
				firstPageConsumed();
			}
		}
		
		private void init(ResultSet rs, int size) {
			this.resultSet = rs;
			this.rowIter = resultSet.iterator();
			if (size > 0) {
				fetchSize = size;
			}

		}
		
		private ResultSet getResultSet() {
			return this.resultSet;
		}
	}
	
	public Object getRowKey() {
		return rowKey;
	}
	
	public CqlColumnSlice<C> getColumnSlice() {
		return columnSlice;
	}

	public CompositeByteBufferRange getCompositeRange() {
		return compositeRange;
	}
	
	public RowQueryType getQueryType() {
		return queryType;
	}
	
	public boolean isPaginating() {
		return paginationContext.isPaginating();
	}
 }
