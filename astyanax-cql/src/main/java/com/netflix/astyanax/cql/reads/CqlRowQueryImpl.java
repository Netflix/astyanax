package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;

public class CqlRowQueryImpl<K, C> implements RowQuery<K, C> {

	private final ChainedContext context; 
	private final ColumnFamily<K, C> cf;
	private final CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	private CompositeByteBufferRange compositeRange;
	private final PaginationContext paginationContext = new PaginationContext(columnSlice);
	

	public CqlRowQueryImpl(ChainedContext ctx) {
		this.context = ctx.rewindForRead();
		this.cf = this.context.skip().skip().getNext(ColumnFamily.class);
	}

	@Override
	public OperationResult<ColumnList<C>> execute() throws ConnectionException {
		
		if (paginationContext.isPaginating() && paginationContext.lastPageConsumed()) {
			return new CqlOperationResultImpl<ColumnList<C>>(null, new CqlColumnListImpl<C>());
		}
		return new InternalRowQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<ColumnList<C>>> executeAsync() throws ConnectionException {
		// TODO: need to add in support for pagination, like above
		return new InternalRowQueryExecutionImpl().executeAsync();
	}

	@Override
	public ColumnQuery<C> getColumn(C column) {
		return new CqlColumnQueryImpl<C>(context.clone().add(column));
	}

	@Override
	public RowQuery<K, C> withColumnSlice(Collection<C> columns) {
		this.columnSlice.setColumns(columns);
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnSlice(C... columns) {
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
			
		Serializer<C> colSerializer = cf.getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		return this.withColumnRange(start, end, reversed, limit);
	}

	@Override
	public RowQuery<K, C> withColumnRange(ByteBufferRange range) {
		
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
		if (enabled) {
			if (columnSlice.getLimit() != Integer.MAX_VALUE) {
				// we ask for limit+1 since ranges have overlapping boundaries.
				this.columnSlice.setLimit(columnSlice.getLimit()+1);
			}
		}
		return this;
	}

	@Override
	public RowCopier<K, C> copyTo(ColumnFamily<K, C> columnFamily, K rowKey) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnCountQuery getCount() {
		return new CqlColumnCountQueryImpl(this.context.clone(), new InternalRowQueryExecutionImpl().getQuery());
	}
	
	private boolean isCompositeRangeQuery() {
		return this.compositeRange != null;
	}

	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<ColumnList<C>> {

		private final Object rowKey; 
		
		public InternalRowQueryExecutionImpl() {
			super(context);
			this.rowKey = context.getNext(Object.class); 
		}

		@Override
		public Query getQuery() {

			if (CqlFamilyFactory.OldStyleThriftMode()) {
				
				if (columnSlice.isColumnSelectQuery()) {
					return new OldStyle().getSelectColumnsQuery();
				} else if (columnSlice.isRangeQuery()) {
					return new OldStyle().getSelectColumnRangeQuery();
				} else if (compositeRange != null) {
					return new OldStyle().getSelectCompositeColumnRangeQuery();
				} else if (columnSlice.isSelectAllQuery()) {
					return new OldStyle().getSelectEntireRowQuery();
				} else {
					throw new IllegalStateException("Undefined query type");
				}
				
			} else {
				
				if (columnSlice.isColumnSelectQuery()) {
					return new NewStyle().getSelectColumnsQuery();
				} else if (columnSlice.isRangeQuery()) {
					return new NewStyle().getSelectColumnRangeQuery();
				} else if (compositeRange != null) {
					throw new NotImplementedException();
				} else if (columnSlice.isSelectAllQuery()) {
					return new NewStyle().getSelectEntireRowQuery();
				} else {
					throw new IllegalStateException("Undefined query type");
				}
			}
		}

		@Override
		public ColumnList<C> parseResultSet(ResultSet rs) {

			List<Row> rows = rs.all(); 

			if (CqlFamilyFactory.OldStyleThriftMode()) {
				CqlColumnListImpl<C> columnList = new CqlColumnListImpl<C>(rows, cf);
				ColumnList<C> newColumnList = paginationContext.trackLastColumn(columnList);
				return newColumnList;
			} else {
				Preconditions.checkArgument(rows.size() <= 1, "Multiple rows returned for row query");
				return new CqlColumnListImpl<C>(rows.get(0));
			}
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}
		
		
		/** OLD STYLE QUERIES */ 
		
		private class OldStyle {
			
			Query getSelectEntireRowQuery() {
				return QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(eq("key", rowKey));
			}
			
			Query getSelectColumnsQuery() {
				
				// Make sure that we are not attempting to do a composite range query
				if (isCompositeRangeQuery()) {
					throw new RuntimeException("Cannot perform composite column slice using column set, use CompositeRangeBuilder instead");
				}
				
				Collection<C> cols = columnSlice.getColumns(); 
				Object[] columns = cols.toArray(new Object[cols.size()]); 
				
				return QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(eq("key", rowKey))
						.and(in("column1", columns));
			}

			Query getSelectColumnRangeQuery() {

				Where stmt = QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(eq("key", rowKey));

				if (columnSlice.getStartColumn() != null) {
					stmt.and(gte("column1", columnSlice.getStartColumn()));
				}

				if (columnSlice.getEndColumn() != null) {
					stmt.and(lte("column1", columnSlice.getEndColumn()));
				}

				if (columnSlice.getReversed()) {
					stmt.orderBy(desc("column1"));
				}

				if (columnSlice.getLimit() != -1) {
					stmt.limit(columnSlice.getLimit());
				}

				return stmt;
			}
			
			Query getSelectCompositeColumnRangeQuery() {

				Where stmt = QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(eq("key", rowKey));

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
			
		}
	
		
		/** NEW STYLE QUERIES */
		private class NewStyle {
			
			Query getSelectEntireRowQuery() {
				return QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));
			}
			
			Query getSelectColumnsQuery() {
				
				// Make sure that we are not attempting to do a composite range query
				if (isCompositeRangeQuery()) {
					throw new RuntimeException("Cannot perform composite column slice using column set, use CompositeRangeBuilder instead");
				}

				Collection<C> columns = columnSlice.getColumns(); 
				Selection selection = QueryBuilder.select();

				for (C column : columns) {
					selection.column(String.valueOf(column));
				}
				return selection.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));
			}
			
			Query getSelectColumnRangeQuery() {

				if (isCompositeRangeQuery()) {
					throw new NotImplementedException();
				}
				
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
					stmt.orderBy(desc(columnSlice.getColumnName()));
				}

				if (columnSlice.getLimit() != -1) {
					stmt.limit(columnSlice.getLimit());
				}
				
				return stmt;
			}
		}
		
	}
	
	private class PaginationContext {
		
		private boolean lastPageConsumed = false; 
		private boolean paginate = false;
		private final CqlColumnSlice<C> columnSlice;
		
		private PaginationContext(CqlColumnSlice<C> columnSlice) {
			this.columnSlice = columnSlice;
		}
		
		private ColumnList<C> trackLastColumn(ColumnList<C> columnList) {
			
			if (!paginate) {
				return columnList;
			}
			
			if (columnList.isEmpty()) {
				lastPageConsumed = true;
				return columnList;
			}
			
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
			
			// Else set up the new range for the next range query
			CqlRangeImpl<C> newRange = 
					new CqlRangeImpl<C>("column1", lastColumn.getName(), columnSlice.getEndColumn(), columnSlice.getLimit(), columnSlice.getReversed());
			
			this.columnSlice.setCqlRange(newRange);
			
			// Now remove the first column, since it is repeating
			ColumnList<C> result = columnList; 
			if (!lastPageConsumed) {
				List<Column<C>> newList = new ArrayList<Column<C>>();
				// skip the first column
				int index = 0; 
				while (index < (columnList.size()-1)) {
					newList.add(columnList.getColumnByIndex(index));
					index++;
				}

				result = new CqlColumnListImpl<C>(newList);
			}
			
			return result;
		}
		
		private boolean isPaginating() {
			return paginate;
		}
		
		private boolean lastPageConsumed() {
			return lastPageConsumed;
		}
	}
	
//	private boolean isSingleEqualityRecord(RangeQueryRecord oldRecord) {
//		
//		List<RangeQueryOp> ops  = oldRecord.getOps();
//		if (ops.size() != 1) {
//			return false;
//		}
//		Equality operator = ops.get(0).getOperator();
//		if (operator != Equality.EQUAL) {
//			return false;
//		}
//		return true;
//	}
//	
//	private boolean checkSingleRecord() {
//		
//	}
}
