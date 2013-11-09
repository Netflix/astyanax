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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.ConsistencyLevelMapping;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.direct.DirectCqlPreparedStatement;
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
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;

public class CqlRowQueryImpl<K, C> implements RowQuery<K, C> {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<K,C> cfContext;

	private final K rowKey;
	private final CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	private CompositeByteBufferRange compositeRange;
	private final PaginationContext paginationContext = new PaginationContext(columnSlice);

	private com.datastax.driver.core.ConsistencyLevel cl = com.datastax.driver.core.ConsistencyLevel.ONE;

	private DirectCqlPreparedStatement preparedStatement = null; 
	
	public CqlRowQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, K rKey, ConsistencyLevel clLevel) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowKey = rKey;
		this.cl = ConsistencyLevelMapping.getCL(clLevel);
		this.paginationContext.initClusteringKeyColumn(cfContext.getColumnFamily());
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
		if (paginationContext.isPaginating() && paginationContext.lastPageConsumed()) {
			return new AsyncOperationResult<ColumnList<C>>(null) {
				@Override
				public OperationResult<ColumnList<C>> getOperationResult(ResultSet resultSet) {
					return new CqlOperationResultImpl<ColumnList<C>>(resultSet, new CqlColumnListImpl<C>());
				}
			};
		}
		return new InternalRowQueryExecutionImpl().executeAsync();
	}

	@Override
	public ColumnQuery<C> getColumn(C column) {
		return new CqlColumnQueryImpl<C>(ksContext, cfContext, rowKey, column);
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

		Serializer<C> colSerializer = cfContext.getColumnFamily().getColumnSerializer();
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
		return this;
	}

	@Override
	public RowCopier<K, C> copyTo(ColumnFamily<K, C> columnFamily, K rowKey) {
		return new CqlRowCopier<K,C>(columnFamily, rowKey, this, ksContext);
	}

	@Override
	public ColumnCountQuery getCount() {
		return new CqlColumnCountQueryImpl(ksContext, cfContext, new InternalRowQueryExecutionImpl().getQuery());
	}

	private boolean isCompositeRangeQuery() {
		return this.compositeRange != null;
	}

	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<ColumnList<C>> {

		private final CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();

		private final String partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		private final String[] allPkColumnNames = cfDef.getAllPkColNames();
		private final List<ColumnDefinition> clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		private final List<ColumnDefinition> regularCols = cfDef.getRegularColumnDefinitionList();
		
		public InternalRowQueryExecutionImpl() {
			super(ksContext, cfContext);
		}

		@Override
		public Statement getQuery() {

			boolean pStatementProvided = preparedStatement != null;
			
			Statement query; 
			
			if (columnSlice.isColumnSelectQuery()) {
				query = pStatementProvided ? bindSelectColumnsQuery() : getSelectColumnsQuery();
			} else if (columnSlice.isRangeQuery()) {
				query = pStatementProvided ? bindSelectColumnRangeQuery() : getSelectColumnRangeQuery();
			} else if (compositeRange != null) {
				query = pStatementProvided ? bindSelectCompositeColumnRangeQuery() : getSelectCompositeColumnRangeQuery();
			} else if (columnSlice.isSelectAllQuery()) {
				query = pStatementProvided ? bindSelectEntireRowQuery() : getSelectEntireRowQuery();
			} else {
				throw new IllegalStateException("Undefined query type");
			}
			
			query.setConsistencyLevel(cl);
			return query;
		}
		
		@Override
		public ColumnList<C> parseResultSet(ResultSet rs) {

			List<Row> rows = rs.all(); 

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

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}

		Statement getSelectEntireRowQuery() {
			
			Selection select = QueryBuilder.select();
			
			for (int i=0; i<allPkColumnNames.length; i++) {
				select.column(allPkColumnNames[i]);
			}
			
			for (ColumnDefinition colDef : regularCols) {
				String colName = colDef.getName();
				select.column(colName).ttl(colName).writeTime(colName);
			}
			return select.from(keyspace, cf.getName()).where(eq(partitionKeyCol, rowKey));
		}

		Statement bindSelectEntireRowQuery() {
			
			PreparedStatement pStmt = preparedStatement.getInnerPreparedStatement();
			return pStmt.bind(rowKey);
		}

		Statement getSelectColumnsQuery() {

			// Make sure that we are not attempting to do a composite range query
			if (isCompositeRangeQuery()) {
				throw new RuntimeException("Cannot perform composite column slice using column set");
			}

			if (clusteringKeyCols.size() == 0) {

				// THIS IS A SIMPLE QUERY WHERE THE INDIVIDUAL COLS ARE BEING SELECTED E.G NAME, AGE ETC
				Select.Selection select = QueryBuilder.select();
				select.column(partitionKeyCol);

				for (C col : columnSlice.getColumns()) {
					String columnName = (String)col;
					select.column(columnName).ttl(columnName).writeTime(columnName);
				}

				return select.from(keyspace, cf.getName()).where(eq(partitionKeyCol, rowKey));

			} else if (clusteringKeyCols.size() > 0) {

				// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES
				Collection<C> cols = columnSlice.getColumns(); 
				Object[] columns = cols.toArray(new Object[cols.size()]); 

				Selection select = QueryBuilder.select();
				
				for (int i=0; i<allPkColumnNames.length; i++) {
					select.column(allPkColumnNames[i]);
				}
				
				for (ColumnDefinition colDef : regularCols) {
					String colName = colDef.getName();
					select.column(colName).ttl(colName).writeTime(colName);
				}

				return select
						.from(keyspace, cf.getName())
						.where(eq(partitionKeyCol, rowKey))
						.and(in(clusteringKeyCols.get(0).getName(), columns));
			} else {
				throw new RuntimeException("Composite col query - todo");
			}
		}

		Statement bindSelectColumnsQuery() {

			// Make sure that we are not attempting to do a composite range query
			if (isCompositeRangeQuery()) {
				throw new RuntimeException("Cannot perform composite column slice using column set");
			}

			PreparedStatement pStmt = preparedStatement.getInnerPreparedStatement();
			
			if (clusteringKeyCols.size() == 0) {

				// THIS IS A SIMPLE QUERY WHERE THE INDIVIDUAL COLS ARE BEING SELECTED E.G NAME, AGE ETC
				// e.g  Select * from ks.table1 where key = ?
				// hence we only need to bind the row key here.
				
				return pStmt.bind(rowKey);

			} else if (clusteringKeyCols.size() > 0) {

				// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES
				// e.g   select * from ks.table1 where key = ? and column1 in (?,?,....?)
				List<Object> objects = new ArrayList<Object>();
				objects.add(rowKey);
				for (Object col : columnSlice.getColumns()) {
					objects.add(col);
				}
				return pStmt.bind(objects.toArray(new Object[objects.size()]));
				
			} else {
				throw new RuntimeException("Composite col query - todo");
			}
		}

		Statement getSelectColumnRangeQuery() {

			if (clusteringKeyCols.size() == 0) {
				throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
			}

			Selection select = QueryBuilder.select();
			
			for (int i=0; i<allPkColumnNames.length; i++) {
				select.column(allPkColumnNames[i]);
			}
			
			for (ColumnDefinition colDef : regularCols) {
				String colName = colDef.getName();
				select.column(colName).ttl(colName).writeTime(colName);
			}
			
			Where where = select.from(keyspace, cf.getName())
				  .where(eq(partitionKeyCol, rowKey));

			String clusterKeyCol = clusteringKeyCols.get(0).getName();
			
			if (columnSlice.getStartColumn() != null) {
				where.and(gte(clusterKeyCol, columnSlice.getStartColumn()));
			}

			if (columnSlice.getEndColumn() != null) {
				where.and(lte(clusterKeyCol, columnSlice.getEndColumn()));
			}

			if (columnSlice.getReversed()) {
				where.orderBy(desc(clusterKeyCol));
			}

			if (columnSlice.getLimit() != -1) {
				where.limit(columnSlice.getLimit());
			}

			return where;
		}

		Statement bindSelectColumnRangeQuery() {

			if (clusteringKeyCols.size() == 0) {
				throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
			}

			PreparedStatement pStmt = preparedStatement.getInnerPreparedStatement();
			
			List<Object> values = new ArrayList<Object>();
			values.add(rowKey);
			
			if (columnSlice.getStartColumn() != null) {
				values.add(columnSlice.getStartColumn());
			}

			if (columnSlice.getEndColumn() != null) {
				values.add(columnSlice.getEndColumn());
			}

			if (columnSlice.getLimit() != -1) {
				values.add(columnSlice.getLimit());
			}

			return pStmt.bind(values.toArray(new Object[values.size()]));
		}

		Statement getSelectCompositeColumnRangeQuery() {

			Selection select = QueryBuilder.select();
			
			for (int i=0; i<allPkColumnNames.length; i++) {
				select.column(allPkColumnNames[i]);
			}
			
			for (ColumnDefinition colDef : regularCols) {
				String colName = colDef.getName();
				select.column(colName).ttl(colName).writeTime(colName);
			}

			Where stmt = select.from(keyspace, cf.getName())
					.where(eq(partitionKeyCol, rowKey));

			List<RangeQueryRecord> records = compositeRange.getRecords();

			int componentIndex = 0; 

			for (RangeQueryRecord record : records) {


				for (RangeQueryOp op : record.getOps()) {

					String columnName = clusteringKeyCols.get(componentIndex).getName();
					switch (op.getOperator()) {

					case EQUAL:
						stmt.and(eq(columnName, op.getValue()));
						componentIndex++;
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
			}
			return stmt;
		}
		

		Statement bindSelectCompositeColumnRangeQuery() {

			PreparedStatement pStmt = preparedStatement.getInnerPreparedStatement();
			List<RangeQueryRecord> records = compositeRange.getRecords();
			
			List<Object> values = new ArrayList<Object>();
			values.add(rowKey);

			for (RangeQueryRecord record : records) {

				for (RangeQueryOp op : record.getOps()) {

					switch (op.getOperator()) {

					case EQUAL:
						values.add(op.getValue());
						break;
					case LESS_THAN :
						values.add(op.getValue());
						break;
					case LESS_THAN_EQUALS:
						values.add(op.getValue());
						break;
					case GREATER_THAN:
						values.add(op.getValue());
						break;
					case GREATER_THAN_EQUALS:
						values.add(op.getValue());
						break;
					default:
						throw new RuntimeException("Cannot recognize operator: " + op.getOperator().name());
					}; // end of switch stmt
				} // end of inner for for ops for each range query record
			}
			return pStmt.bind(values.toArray(new Object[values.size()]));
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
		
		private boolean isFirstPage() {
			return isFirstPage;
		}
	}

	@Override
	public RowQuery<K, C> withPreparedStatement(CqlPreparedStatement pStatement) {
		this.preparedStatement = (DirectCqlPreparedStatement) pStatement;
		return this;
	}

	@Override
	public CqlPreparedStatement asPreparedStatement() {
		this.preparedStatement = null;
		RegularStatement stmt = (RegularStatement) new InternalRowQueryExecutionImpl().getQuery();
		Session session = ksContext.getSession();
		PreparedStatement pStmt = session.prepare(stmt.getQueryString());
		this.preparedStatement = new DirectCqlPreparedStatement(session, pStmt);
		return this.preparedStatement;
	}
}
