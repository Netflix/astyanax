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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.direct.DirectCqlPreparedStatement;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.reads.model.CqlRangeImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowListImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowListIterator;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice.RowRange;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
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

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<K,C> cfContext;

	private final CqlRowSlice<K> rowSlice;
	private CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	
	private CompositeByteBufferRange compositeRange = null;

	private DirectCqlPreparedStatement preparedStatement = null; 

	private final boolean isPaginating; 
	private boolean useCaching = false;
	
	public enum RowSliceQueryType {
		RowKeys, RowRange
	}

	public enum ColumnSliceQueryType {
		AllColumns, ColumnSet, ColumnRange; 
	}
	
	private final RowSliceQueryType rowQueryType;
	private ColumnSliceQueryType colQueryType = ColumnSliceQueryType.AllColumns;
	
	public CqlRowSliceQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, CqlRowSlice<K> rSlice, boolean useCaching) {
		this(ksCtx, cfCtx, rSlice, true, useCaching);
	}

	public CqlRowSliceQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, CqlRowSlice<K> rSlice, boolean condition, boolean useCaching) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowSlice = rSlice;
		this.isPaginating = condition;
		this.rowQueryType = (rowSlice.isRangeQuery()) ? RowSliceQueryType.RowRange : RowSliceQueryType.RowKeys;
		this.useCaching = useCaching;
	}

	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		return new InternalRowQueryExecutionImpl(this).execute();
	}

	@Override
	public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
		return new InternalRowQueryExecutionImpl(this).executeAsync();
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(C... columns) {
		colQueryType = ColumnSliceQueryType.ColumnSet;
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(Collection<C> columns) {
		colQueryType = ColumnSliceQueryType.ColumnSet;
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(ColumnSlice<C> columns) {
		colQueryType = ColumnSliceQueryType.ColumnSet;
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		colQueryType = ColumnSliceQueryType.ColumnRange;
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
		colQueryType = ColumnSliceQueryType.ColumnRange;
		Serializer<C> colSerializer = cfContext.getColumnFamily().getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		return this.withColumnRange(start, end, reversed, limit);
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {

		colQueryType = ColumnSliceQueryType.ColumnRange;

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
		Statement query = new InternalRowQueryExecutionImpl(this).getQuery();
		return new CqlRowSliceColumnCountQueryImpl<K>(ksContext, cfContext, query);
	}

	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<Rows<K, C>> {

		private final CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		private final CqlRowSliceQueryImpl<K, C> rowSliceQuery; 
		
		public InternalRowQueryExecutionImpl(CqlRowSliceQueryImpl<K, C> rSliceQuery) {
			super(ksContext, cfContext);
			this.rowSliceQuery = rSliceQuery;
		}
		
		public InternalRowQueryExecutionImpl(KeyspaceContext ksContext, ColumnFamilyMutationContext<?, ?> cfContext) {
			super(ksContext, cfContext);
			this.rowSliceQuery = null;
		}

		@Override
		public Statement getQuery() {
			return cfDef.getRowQueryGenerator().getQueryStatement(rowSliceQuery, useCaching);
		}

		@Override
		public Rows<K, C> parseResultSet(ResultSet rs) throws NotFoundException {
			if (!isPaginating) {
				List<com.datastax.driver.core.Row> rows = rs.all();
				if (rows == null || rows.isEmpty()) {
					return new CqlRowListImpl<K, C>();
				}
				return new CqlRowListImpl<K, C>(rows, (ColumnFamily<K, C>) cf);
			} else {
				if (rs == null) {
					return new CqlRowListImpl<K, C>();
				}
				return new CqlRowListIterator<K, C>(rs, (ColumnFamily<K, C>) cf);
			}
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}
		
	}
		private class InternalRowQueryExecutionImpl2 extends CqlAbstractExecutionImpl<Rows<K, C>> {

		private final CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();

		String partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		List<ColumnDefinition> clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		List<ColumnDefinition> regularCols = cfDef.getRegularColumnDefinitionList();
		String[] allPkColumnNames = cfDef.getAllPkColNames();
		
		private boolean prepareStatement = false; 
		
		public InternalRowQueryExecutionImpl2() {
			super(ksContext, cfContext);
		}

		public InternalRowQueryExecutionImpl2(boolean condition) {
			super(ksContext, cfContext);
			this.prepareStatement = condition;
		}

		@Override
		public Statement getQuery() {

			boolean isPStmtProvided = preparedStatement != null;

			if (rowSlice.isCollectionQuery()) {

				if (compositeRange != null) {
					return isPStmtProvided ? bindCompositeColumnRangeForRowKeys() : selectCompositeColumnRangeForRowKeys();
				}

				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return isPStmtProvided ? bindAllColumnsForRowKeys() : selectAllColumnsForRowKeys();
				case COLUMN_COLLECTION:
					return isPStmtProvided ? bindColumnSetForRowKeys() : selectColumnSetForRowKeys();
				case COLUMN_RANGE:
					return isPStmtProvided ? bindColumnRangeForRowKeys() : selectColumnRangeForRowKeys();
				default:
					throw new IllegalStateException();
				}
			} else {

				if (compositeRange != null) {
					return isPStmtProvided ? bindCompositeColumnRangeForRowRange() : selectCompositeColumnRangeForRowRange();
				}

				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return isPStmtProvided ? bindAllColumnsForRowRange() : selectAllColumnsForRowRange();
				case COLUMN_COLLECTION:
					return isPStmtProvided ? bindColumnSetForRowRange() : selectColumnSetForRowRange();
				case COLUMN_RANGE:
					return isPStmtProvided ? bindColumnRangeForRowRange() : selectColumnRangeForRowRange();
				default:
					throw new IllegalStateException();
				}
			}
		}

		@Override
		public Rows<K, C> parseResultSet(ResultSet rs) {
			if (!isPaginating) {
				List<com.datastax.driver.core.Row> rows = rs.all();
				if (rows == null || rows.isEmpty()) {
					return new CqlRowListImpl<K, C>();
				}
				return new CqlRowListImpl<K, C>(rows, (ColumnFamily<K, C>) cf);
			} else {
				if (rs == null) {
					return new CqlRowListImpl<K, C>();
				}
				return new CqlRowListIterator<K, C>(rs, (ColumnFamily<K, C>) cf);
			}
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}

		/** ALL ROW RANGE COLLECTION QUERIES HERE */ 

		private Statement selectAllColumnsForRowKeys() {

			Select select = selectAllColumnsFromKeyspaceAndCF();
			return select.where(in(partitionKeyCol, rowSlice.getKeys().toArray()));
		}

		private Statement bindAllColumnsForRowKeys() {

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			return pStatement.bind(rowSlice.getKeys().toArray());
		}

		private Statement selectColumnRangeForRowKeys() {

			Select select = selectAllColumnsFromKeyspaceAndCF();
			Where where = select.where(in(partitionKeyCol, rowSlice.getKeys().toArray()));
			where = addWhereClauseForColumn(where, columnSlice);
			return where;
		}

		private Statement bindColumnRangeForRowKeys() {

			List<Object> values = new ArrayList<Object>();
			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();

			values.addAll(rowSlice.getKeys());
			bindWhereClauseForColumnRange(values, columnSlice);

			return pStatement.bind(values.toArray());
		}

		private Statement selectColumnSetForRowKeys() {

			Collection<K> rowKeys = rowSlice.getKeys();
			Collection<C> cols = columnSlice.getColumns();

			if (clusteringKeyCols.size() == 0) {

				// THIS IS A SIMPLE QUERY WHERE THE INDIVIDUAL COLS ARE BEING SELECTED E.G NAME, AGE ETC
				Select.Selection select = QueryBuilder.select();
				select.column(partitionKeyCol);

				for (C col : cols) {
					String columnName = (String)col; 
					select.column(columnName).ttl(columnName).writeTime(columnName);
				}

				return select.from(keyspace, cf.getName()).where(in(partitionKeyCol, rowKeys.toArray()));

			} else if (clusteringKeyCols.size() > 0) {

				// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES
				Object[] columns = cols.toArray(new Object[cols.size()]); 

				String clusteringCol = clusteringKeyCols.get(0).getName();

				Select select = selectAllColumnsFromKeyspaceAndCF();
				return select.where(in(partitionKeyCol, rowKeys.toArray()))
						.and(in(clusteringCol, columns));
			} else {
				throw new RuntimeException("Composite col query - todo");
			}
		}

		private Statement bindColumnSetForRowKeys() {

			List<Object> values = new ArrayList<Object>();
			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();

			if (clusteringKeyCols.size() == 0) {
				values.addAll(rowSlice.getKeys());

			} else if (clusteringKeyCols.size() > 0) {

				values.addAll(rowSlice.getKeys());
				values.addAll(columnSlice.getColumns());
			} else {
				throw new RuntimeException("Composite col query - todo");
			}

			return pStatement.bind(values.toArray());
		}

		private Statement selectCompositeColumnRangeForRowKeys() {

			Select select = selectAllColumnsFromKeyspaceAndCF();
			Where stmt = select.where(in(partitionKeyCol, rowSlice.getKeys().toArray()));
			stmt = addWhereClauseForCompositeColumnRange(stmt, compositeRange);
			return stmt;
		}

		private Statement bindCompositeColumnRangeForRowKeys() {

			List<Object> values = new ArrayList<Object>();
			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();

			values.addAll(rowSlice.getKeys());
			bindWhereClauseForCompositeColumnRange(values, compositeRange);

			return pStatement.bind(values.toArray());
		}

		/** ALL ROW RANGE QUERIES FROM HERE ON */ 
		private Statement selectAllColumnsForRowRange() {

			Select select = selectAllColumnsFromKeyspaceAndCF();
			return addWhereClauseForRowKey(partitionKeyCol, select, rowSlice.getRange());
		}

		private Statement bindAllColumnsForRowRange() {

			List<Object> values = new ArrayList<Object>();
			bindWhereClauseForRowRange(values, rowSlice.getRange());

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			System.out.println("pStatement: " + pStatement.getQueryString());
			return pStatement.bind(values.toArray()); 
		}

		private Statement selectColumnSetForRowRange() {

			RowRange<K> range = rowSlice.getRange();
			Collection<C> cols = columnSlice.getColumns();

			if (clusteringKeyCols.size() == 0) {

				// THIS IS A SIMPLE QUERY WHERE THE INDIVIDUAL COLS ARE BEING SELECTED E.G NAME, AGE ETC
				Select.Selection select = QueryBuilder.select();
				select.column(partitionKeyCol);

				for (C col : cols) {
					String columnName = (String)col;
					select.column(columnName).ttl(columnName).writeTime(columnName);
				}

				Select selection = select.from(keyspace, cf.getName());
				Where where = addWhereClauseForRowKey(partitionKeyCol, selection, range);
				return where;

			} else if (clusteringKeyCols.size() > 0) {

				// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES

				Object[] columns = cols.toArray(new Object[cols.size()]); 

				Select select = selectAllColumnsFromKeyspaceAndCF();

				if (columns != null && columns.length > 0) {
					select.allowFiltering();
				}
				Where where = addWhereClauseForRowKey(partitionKeyCol, select, range);
				where.and(in(clusteringKeyCols.get(0).getName(), columns));

				return where;
			} else {
				throw new RuntimeException("Invalid row slice query combination");
			}
		}

		private Statement bindColumnSetForRowRange() {

			List<Object> values = new ArrayList<Object>();

			RowRange<K> range = rowSlice.getRange();
			Collection<C> cols = columnSlice.getColumns();

			if (clusteringKeyCols.size() == 0) {
				bindWhereClauseForRowRange(values, range);

			} else if (clusteringKeyCols.size() > 0) {

				bindWhereClauseForRowRange(values, range);
				values.addAll(cols);
			} else {
				throw new RuntimeException("Invalid row slice query combination");
			}

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			return pStatement.bind(values.toArray());
		}

		private Statement selectColumnRangeForRowRange() {

			Select select = selectAllColumnsFromKeyspaceAndCF();
			if (columnSlice != null && columnSlice.isRangeQuery()) {
				select.allowFiltering();
			}

			Where where = addWhereClauseForRowKey(partitionKeyCol, select, rowSlice.getRange());			
			where = addWhereClauseForColumn(where, columnSlice);
			return where;
		}

		private Statement bindColumnRangeForRowRange() {

			List<Object> values = new ArrayList<Object>();

			bindWhereClauseForRowRange(values, rowSlice.getRange());
			bindWhereClauseForColumnRange(values, columnSlice);

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			
			System.out.println("pStatement: " + pStatement.getQueryString());
			return pStatement.bind(values.toArray());
		}

		private Statement selectCompositeColumnRangeForRowRange() {

			Select select = selectAllColumnsFromKeyspaceAndCF();
			if (compositeRange != null) {
				select.allowFiltering();
			}

			Where where = addWhereClauseForRowKey(partitionKeyCol, select, rowSlice.getRange());	
			where = addWhereClauseForCompositeColumnRange(where, compositeRange);
			return where;
		}

		private Statement bindCompositeColumnRangeForRowRange() {

			List<Object> values = new ArrayList<Object>();

			bindWhereClauseForRowRange(values, rowSlice.getRange());
			bindWhereClauseForCompositeColumnRange(values, compositeRange);

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			return pStatement.bind(values.toArray());
		}

		private Select selectAllColumnsFromKeyspaceAndCF() {

			Select.Selection select = QueryBuilder.select();
			for (int i=0; i<allPkColumnNames.length; i++) {
				select.column(allPkColumnNames[i]);
			}

			for (ColumnDefinition colDef : regularCols) {
				String colName = colDef.getName();
				select.column(colName).ttl(colName).writeTime(colName);
			}
			return select.from(keyspace, cf.getName());
		}

		private Where addWhereClauseForColumn(Where where, CqlColumnSlice<C> columnSlice) {

			String clusteringKeyCol = clusteringKeyCols.get(0).getName();

			if (!columnSlice.isRangeQuery()) {
				return where;
			}
			if (columnSlice.getStartColumn() != null) {
				where.and(gte(clusteringKeyCol, columnSlice.getStartColumn()));
			}
			if (columnSlice.getEndColumn() != null) {
				where.and(lte(clusteringKeyCol, columnSlice.getEndColumn()));
			}

			if (columnSlice.getReversed()) {
				where.orderBy(desc(clusteringKeyCol));
			}

			if (columnSlice.getLimit() != -1) {
				where.limit(columnSlice.getLimit());
			}

			return where;
		}

		private void bindWhereClauseForColumnRange(List<Object> values, CqlColumnSlice<C> columnSlice) {

			if (!columnSlice.isRangeQuery()) {
				return;
			}
			if (columnSlice.getStartColumn() != null) {
				values.add(columnSlice.getStartColumn());
			}
			if (columnSlice.getEndColumn() != null) {
				values.add(columnSlice.getEndColumn());
			}

			if (columnSlice.getLimit() != -1) {
				values.add(columnSlice.getLimit());
			}

			return;
		}

		private Where addWhereClauseForCompositeColumnRange(Where stmt, CompositeByteBufferRange compositeRange) {

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

		private void bindWhereClauseForCompositeColumnRange(List<Object> values, CompositeByteBufferRange compositeRange) {

			List<RangeQueryRecord> records = compositeRange.getRecords();

			for (RangeQueryRecord record : records) {
				for (RangeQueryOp op : record.getOps()) {
					values.add(op.getValue());
				}
			}
			return;
		}

		private Where addWhereClauseForRowKey(String keyAlias, Select select, RowRange<K> rowRange) {

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

				Object startToken, endToken;
				
				if (prepareStatement) {
					
					startToken = rowRange.getStartToken();
					endToken = rowRange.getEndToken();
					
				} else {
					startToken = rowRange.getStartToken() != null ? new BigInteger(rowRange.getStartToken()) : null; 
					endToken = rowRange.getEndToken() != null ? new BigInteger(rowRange.getEndToken()) : null; 
				}
//				BigInteger startTokenB = rowRange.getStartToken() != null ? new BigInteger(rowRange.getStartToken()) : null; 
//				BigInteger endTokenB = rowRange.getEndToken() != null ? new BigInteger(rowRange.getEndToken()) : null; 

				
//				Long startToken = startTokenB.longValue();
//				Long endToken = endTokenB.longValue();

				if (startToken != null && endToken != null) {

					where = select.where(gte(tokenOfKey, startToken))
							.and(lte(tokenOfKey, endToken));

				} else if (startToken != null) {
					where = select.where(gte(tokenOfKey, startToken));

				} else if (endToken != null) {
					where = select.where(lte(tokenOfKey, endToken));
				}

//				System.out.println(rowRange.getStartToken());
//				System.out.println(rowRange.getEndToken());
//				where = select.where(gte(tokenOfKey, rowRange.getStartToken()));
//				where.and(lte(tokenOfKey, rowRange.getEndToken()));
			} else { 
				where = select.where();
			}

			if (rowRange.getCount() > 0) {
				// TODO: fix this
				//where.limit(rowRange.getCount());
			}
			
			
			System.out.println("Where : " + where.getQueryString());

			return where; 
		}

		private void bindWhereClauseForRowRange(List<Object> values, RowRange<K> rowRange) {

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
				if (rowRange.getStartKey() != null) {
					values.add(rowRange.getStartKey());
				}
				if (rowRange.getEndKey() != null) {
					values.add(rowRange.getEndKey());
				}

			} else if (tokenIsPresent) {

				BigInteger startTokenB = rowRange.getStartToken() != null ? new BigInteger(rowRange.getStartToken()) : null; 
				BigInteger endTokenB = rowRange.getEndToken() != null ? new BigInteger(rowRange.getEndToken()) : null; 

				Long startToken = startTokenB.longValue();
				Long endToken = endTokenB.longValue();
				
				if (startToken != null && endToken != null) {
					if (startToken != null) {
						values.add(startToken);
					}
					if (endToken != null) {
						values.add(endToken);
					}
				}

				if (rowRange.getCount() > 0) {
					// TODO: fix this
					//where.limit(rowRange.getCount());
				}
				return; 
			}
		}

	}

	@Override
	public RowSliceQuery<K, C> withPreparedStatement(CqlPreparedStatement preparedStatement) {
		this.preparedStatement = (DirectCqlPreparedStatement) preparedStatement;
		return this;
	}

	@Override
	public CqlPreparedStatement asPreparedStatement() {
		this.preparedStatement = null;
		RegularStatement stmt = (RegularStatement) new InternalRowQueryExecutionImpl(this).getQuery();
		Session session = ksContext.getSession();
		PreparedStatement pStmt = session.prepare(stmt.getQueryString());
		this.preparedStatement = new DirectCqlPreparedStatement(session, pStmt);
		return this.preparedStatement;
	}
	
	
	public CqlRowSlice<K> getRowSlice() {
		return rowSlice;
	}
	
	public CqlColumnSlice<C> getColumnSlice() {
		return columnSlice;
	}

	public CompositeByteBufferRange getCompositeRange() {
		return compositeRange;
	}
	
	public ColumnSliceQueryType getColQueryType() {
		return colQueryType;
	}

	public RowSliceQueryType getRowQueryType() {
		return rowQueryType;
	}
}


