package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
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
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;
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
	private final ColumnFamilyMutationContext<K,C> cfContext;

	private final K rowKey;
	private final CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	private CompositeByteBufferRange compositeRange;
	private final PaginationContext paginationContext = new PaginationContext(columnSlice);

	public enum RowQueryType {
		AllColumns, ColumnSlice, ColumnRange, SingleColumn; 
	}
	
	private RowQueryType queryType = RowQueryType.AllColumns;  // The default
	private boolean useCaching = false;
	
	public CqlRowQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, K rKey, ConsistencyLevel clLevel, boolean useCaching) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowKey = rKey;
		this.paginationContext.initClusteringKeyColumn(cfContext.getColumnFamily());
		this.useCaching = useCaching;
	}

	@Override
	public OperationResult<ColumnList<C>> execute() throws ConnectionException {

		if (paginationContext.isPaginating() && paginationContext.lastPageConsumed()) {
			return new CqlOperationResultImpl<ColumnList<C>>(null, new CqlColumnListImpl<C>());
		}

		return new InternalRowQueryExecutionImpl(this).execute();
	}

	@Override
	public ListenableFuture<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
		if (paginationContext.isPaginating() && paginationContext.lastPageConsumed()) {
			return new AsyncOperationResult<ColumnList<C>>(null) {
				@Override
				public OperationResult<ColumnList<C>> getOperationResult(ResultSet resultSet) {
					return new CqlOperationResultImpl<ColumnList<C>>(resultSet, new CqlColumnListImpl<C>());
				}
			};
		}
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
		paginationContext.paginate = enabled;
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
			
			return cfDef.getRowQueryGenerator().getQueryStatement(rowQuery, useCaching);
		}

		@Override
		public ColumnList<C> parseResultSet(ResultSet resultSet) throws NotFoundException {
			List<Row> rows = resultSet.all(); 

			if (rows == null || rows.isEmpty()) {
				if (paginationContext.isPaginating()) {
					paginationContext.lastPageConsumed = true;
				}
				return new CqlColumnListImpl<C>();
			}
			
			if (allPkColumnNames.length == 1 || regularCols.size() > 1) {
				CqlColumnListImpl<C> columnList = new CqlColumnListImpl<C>(rows.get(0), cf);
				return columnList;
			} else {
				CqlColumnListImpl<C> columnList = new CqlColumnListImpl<C>(rows, cf);
				paginationContext.trackLastColumn(columnList);
				return columnList;
			}
		}


	}
	
	private class PaginationContext {

		private boolean lastPageConsumed = false; 
		private boolean paginate = false;
		private boolean isFirstPage = true;
		private String clusteringColName = null; 

		private final CqlColumnSlice<C> columnSlice;
		
		private PaginationContext(CqlColumnSlice<C> columnSlice) {
			this.columnSlice = columnSlice;
		}

		private void initClusteringKeyColumn(ColumnFamily<?,?> cf) {
			CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
			List<ColumnDefinition> clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
			if (clusteringKeyCols.size() != 0) {
				this.clusteringColName = clusteringKeyCols.get(0).getName();
			} else {
				this.clusteringColName = null;
			}
		}
		
		private void trackLastColumn(ColumnList<C> columnList) {

			if (!paginate) {
				return;
			}

			try {
				if (columnList.isEmpty()) {
					lastPageConsumed = true;
					return;
				}
				
				// CHECK IF THIS IS THE LAST PAGE
				Column<C> lastColumn = columnList.getColumnByIndex(columnList.size()-1);

				if (columnSlice.getEndColumn() != null) {
					if (lastColumn.getName().equals(columnSlice.getEndColumn())) {
						// 	this was the last page. Stop paginating. 
						this.lastPageConsumed = true;
					}
				} else {
					if (columnList.size() < columnSlice.getLimit()) {
						this.lastPageConsumed = true;
					}
				}

				// Set up the new range for the next range query
				CqlRangeImpl<C> newRange = 
						new CqlRangeImpl<C>(clusteringColName, lastColumn.getName(), columnSlice.getEndColumn(), columnSlice.getLimit(), columnSlice.getReversed());

				this.columnSlice.setCqlRange(newRange);

				// IN CASE THIS ISN'T THE FIRST PAGE, THEN TRIM THE COLUMNLIST - I.E REMOVE THE FIRST COLUMN
				// SINCE IT IS THE LAST COLUMN OF THE PREVIOUS PAGE
				if (!isFirstPage) {
					((CqlColumnListImpl<C>)columnList).trimFirstColumn();
				}
				return;
			} finally {
				isFirstPage = false;
			}
		}

		private boolean isPaginating() {
			return paginate;
		}

		private boolean lastPageConsumed() {
			return lastPageConsumed;
		}
	}

	public K getRowKey() {
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
 }
