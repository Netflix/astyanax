package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;

public class CFRowQueryGen {

	private final AtomicReference<Session> sessionRef = new AtomicReference<Session>(null);
	private final String keyspace; 
	private final CqlColumnFamilyDefinitionImpl cfDef;

	private final String partitionKeyCol;
	private final String[] allPrimayKeyCols;
	private final List<ColumnDefinition> clusteringKeyCols;
	private final List<ColumnDefinition> regularCols;
	
	private boolean isCompositeColumn; 
	private boolean isFlatTable; 
	
	private static final String BIND_MARKER = "?";

	private final CFRowKeysQueryGen rowKeysQueryGen; 
	private final CFRowRangeQueryGen rowRangeQueryGen; 
	private final FlatTableRowQueryGen flatTableRowQueryGen; 
	private final FlatTableRowSliceQueryGen flatTableRowSliceQueryGen; 
	private final CFColumnQueryGen columnQueryGen; 
	
	public CFRowQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.sessionRef.set(session);

		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		allPrimayKeyCols = cfDef.getAllPkColNames();
		clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		regularCols = cfDef.getRegularColumnDefinitionList();

		isCompositeColumn = (clusteringKeyCols.size() > 1);
		isFlatTable = (clusteringKeyCols.size() == 0);
		
		rowKeysQueryGen = new CFRowKeysQueryGen(session, keyspaceName, cfDefinition);
		rowRangeQueryGen = new CFRowRangeQueryGen(session, keyspaceName, cfDefinition);
		flatTableRowQueryGen = new FlatTableRowQueryGen(session, keyspaceName, cfDefinition);
		flatTableRowSliceQueryGen = new FlatTableRowSliceQueryGen(session, keyspaceName, cfDefinition);
		columnQueryGen = new CFColumnQueryGen(session, keyspaceName, cfDefinition);
	}

	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectEntireRow = new QueryGenCache<CqlRowQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(CqlRowQueryImpl<?, ?> rowQuery) {

			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					Selection select = QueryBuilder.select();

					for (int i=0; i<allPrimayKeyCols.length; i++) {
						select.column(allPrimayKeyCols[i]);
					}

					for (ColumnDefinition colDef : regularCols) {
						String colName = colDef.getName();
						select.column(colName).ttl(colName).writeTime(colName);
					}

					RegularStatement stmt = select.from(keyspace, cfDef.getName()).where(eq(partitionKeyCol, BIND_MARKER));
					return stmt; 
				}
			};
		}


		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowQueryImpl<?, ?> rowQuery) {
			return pStatement.bind(rowQuery.getRowKey());
		}
	};

	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectColumnSliceWithClusteringKey = new QueryGenCache<CqlRowQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowQueryImpl<?, ?> rowQuery) {

			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					if (clusteringKeyCols.size() != 1) {
						throw new RuntimeException("Cannot perform column slice query with clusteringKeyCols.size: " + clusteringKeyCols.size());
					}

					// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES

					Selection select = QueryBuilder.select();

					for (int i=0; i<allPrimayKeyCols.length; i++) {
						select.column(allPrimayKeyCols[i]);
					}

					for (ColumnDefinition colDef : regularCols) {
						String colName = colDef.getName();
						select.column(colName).ttl(colName).writeTime(colName);
					}

					int numCols = rowQuery.getColumnSlice().getColumns().size(); 
					
					List<Object> colSelection = new ArrayList<Object>();
					for (int i=0; i<numCols; i++) {
						colSelection.add(BIND_MARKER);
					}
					
					return select
							.from(keyspace, cfDef.getName())
							.where(eq(partitionKeyCol, BIND_MARKER))
							.and(in(clusteringKeyCols.get(0).getName(), colSelection.toArray(new Object[colSelection.size()])));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowQueryImpl<?, ?> rowQuery) {

			List<Object> objects = new ArrayList<Object>();
			objects.add(rowQuery.getRowKey());
			for (Object col : rowQuery.getColumnSlice().getColumns()) {
				objects.add(col);
			}
			return pStatement.bind(objects.toArray(new Object[objects.size()]));
		}
	};

	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectColumnRangeWithClusteringKey = new QueryGenCache<CqlRowQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowQueryImpl<?, ?> rowQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					if (clusteringKeyCols.size() != 1) {
						throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
					}

					Selection select = QueryBuilder.select();

					for (int i=0; i<allPrimayKeyCols.length; i++) {
						select.column(allPrimayKeyCols[i]);
					}

					for (ColumnDefinition colDef : regularCols) {
						String colName = colDef.getName();
						select.column(colName).ttl(colName).writeTime(colName);
					}

					Where where = select.from(keyspace, cfDef.getName())
							.where(eq(partitionKeyCol, BIND_MARKER));

					String clusterKeyCol = clusteringKeyCols.get(0).getName();

					CqlColumnSlice<?> columnSlice = rowQuery.getColumnSlice();
					if (columnSlice.getStartColumn() != null) {
						where.and(gte(clusterKeyCol, BIND_MARKER));
					}

					if (columnSlice.getEndColumn() != null) {
						where.and(lte(clusterKeyCol, BIND_MARKER));
					}

					if (columnSlice.getReversed()) {
						where.orderBy(desc(clusterKeyCol));
					}
					
					if (!rowQuery.isPaginating()) {
						// Column limits are applicable only when we are not paginating
						if (columnSlice.getLimit() != -1) {
							where.limit(columnSlice.getLimit());
						}
					}

					return where;
				}				
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowQueryImpl<?, ?> rowQuery) {
			if (clusteringKeyCols.size() != 1) {
				throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
			}

			List<Object> values = new ArrayList<Object>();
			values.add(rowQuery.getRowKey());

			CqlColumnSlice<?> columnSlice = rowQuery.getColumnSlice();

			if (columnSlice.getStartColumn() != null) {
				values.add(columnSlice.getStartColumn());
			}

			if (columnSlice.getEndColumn() != null) {
				values.add(columnSlice.getEndColumn());
			}

			return pStatement.bind(values.toArray(new Object[values.size()]));
		}
	};

	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectWithCompositeColumn = new QueryGenCache<CqlRowQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowQueryImpl<?, ?> rowQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					Selection select = QueryBuilder.select();

					for (int i=0; i<allPrimayKeyCols.length; i++) {
						select.column(allPrimayKeyCols[i]);
					}

					for (ColumnDefinition colDef : regularCols) {
						String colName = colDef.getName();
						select.column(colName).ttl(colName).writeTime(colName);
					}

					Where stmt = select.from(keyspace, cfDef.getName())
							.where(eq(partitionKeyCol, BIND_MARKER));

					List<RangeQueryRecord> records = rowQuery.getCompositeRange().getRecords();

					int componentIndex = 0; 

					for (RangeQueryRecord record : records) {


						for (RangeQueryOp op : record.getOps()) {

							String columnName = clusteringKeyCols.get(componentIndex).getName();
							switch (op.getOperator()) {

							case EQUAL:
								stmt.and(eq(columnName, BIND_MARKER));
								componentIndex++;
								break;
							case LESS_THAN :
								stmt.and(lt(columnName, BIND_MARKER));
								break;
							case LESS_THAN_EQUALS:
								stmt.and(lte(columnName, BIND_MARKER));
								break;
							case GREATER_THAN:
								stmt.and(gt(columnName, BIND_MARKER));
								break;
							case GREATER_THAN_EQUALS:
								stmt.and(gte(columnName, BIND_MARKER));
								break;
							default:
								throw new RuntimeException("Cannot recognize operator: " + op.getOperator().name());
							}; // end of switch stmt
						} // end of inner for for ops for each range query record
					}
					return stmt;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowQueryImpl<?, ?> rowQuery) {

			List<RangeQueryRecord> records = rowQuery.getCompositeRange().getRecords();

			List<Object> values = new ArrayList<Object>();
			values.add(rowQuery.getRowKey());

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
			return pStatement.bind(values.toArray(new Object[values.size()]));
		}
	};
	
	
	public Statement getQueryStatement(final CqlRowQueryImpl<?,?> rowQuery, boolean useCaching)  {
		
		if (isFlatTable) {
			return flatTableRowQueryGen.getQueryStatement(rowQuery, useCaching);
		}
		
		switch (rowQuery.getQueryType()) {
		
		case AllColumns:
			return SelectEntireRow.getBoundStatement(rowQuery, useCaching);
		case ColumnSlice:
			return SelectColumnSliceWithClusteringKey.getBoundStatement(rowQuery, useCaching);
		case ColumnRange:
			if (isCompositeColumn) {
				return SelectWithCompositeColumn.getBoundStatement(rowQuery, useCaching);
			} else {
				return SelectColumnRangeWithClusteringKey.getBoundStatement(rowQuery, useCaching);
			}
		default :
			throw new RuntimeException("RowQuery use case not supported. Fix this!!");
		}
	}
	
	public Statement getQueryStatement(final CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching)  {
		
		if (isFlatTable) {
			return flatTableRowSliceQueryGen.getQueryStatement(rowSliceQuery, useCaching);
		}

		switch (rowSliceQuery.getRowQueryType()) {
		
		case RowKeys:
			return rowKeysQueryGen.getQueryStatement(rowSliceQuery, useCaching);
		case RowRange:
			return rowRangeQueryGen.getQueryStatement(rowSliceQuery, useCaching);
		default :
			throw new RuntimeException("RowSliceQuery use case not supported. Fix this!!");
		}
	}

	public Statement getQueryStatement(final CqlColumnQueryImpl<?> columnQuery, boolean useCaching)  {
		
		return columnQueryGen.getQueryStatement(columnQuery, useCaching);
	}
}
