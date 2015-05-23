package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice.RowRange;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.query.RowSliceQuery;

/**
 * Just like {@link FlatTableRowQueryGen} this class encapsulates the functionality for row query generation for 
 * Astyanax {@link RowSliceQuery}(s). 
 * 
 * The class uses a collection of query generators to handle all sort of RowSliceQuery permutations like
 * 1. Selecting all columns for a row collection
 * 2. Selecting a column set for a row collection
 * 3. Selecting all columns for a row range
 * 4. Selecting a column set for a row range
 * 
 * Note that this class supports query generation for flat tables only. 
 * For tables with clustering keys see {@link CFRowKeysQueryGen} and {@link CFRowRangeQueryGen}.
 * 
 * Also, just like the other query generators, use this with caution when using caching of {@link PreparedStatement}
 * See {@link FlatTableRowQueryGen} for a detailed explanation of why PreparedStatement caching will not work for queries
 * that do not have the same signatures. 
 * 
 * @author poberai
 *
 */
public class FlatTableRowSliceQueryGen {

	protected AtomicReference<Session> sessionRef = new AtomicReference<Session>(null);
	protected final String keyspace; 
	protected final CqlColumnFamilyDefinitionImpl cfDef;

	protected final String partitionKeyCol;
	protected final String[] allPrimayKeyCols;
	protected final List<ColumnDefinition> regularCols;
	
	protected static final String BIND_MARKER = "?";
	
	public FlatTableRowSliceQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.sessionRef.set(session);

		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		allPrimayKeyCols = cfDef.getAllPkColNames();
		regularCols = cfDef.getRegularColumnDefinitionList();
	}

	/**
	 * 
	 *   SOME BASIC UTILITY METHODS USED BY ALL THE ROW SLICE QUERY GENERATORS
	 */
	
	protected Select selectAllColumnsFromKeyspaceAndCF() {

		Select.Selection select = QueryBuilder.select();
		for (int i=0; i<allPrimayKeyCols.length; i++) {
			select.column(allPrimayKeyCols[i]);
		}

		for (ColumnDefinition colDef : regularCols) {
			String colName = colDef.getName();
			select.column(colName).ttl(colName).writeTime(colName);
		}
		return select.from(keyspace, cfDef.getName());
	}

	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectAllColumnsForRowKeys = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					
					Select select = selectAllColumnsFromKeyspaceAndCF();
					return select.where(in(partitionKeyCol, rowSliceQuery.getRowSlice().getKeys().toArray()));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return pStatement.bind(rowSliceQuery.getRowSlice().getKeys().toArray());
		}
	};
	
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectColumnSetForRowKeys = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					Select.Selection select = QueryBuilder.select();
					select.column(partitionKeyCol);

					for (Object col : rowSliceQuery.getColumnSlice().getColumns()) {
						String columnName = (String)col; 
						select.column(columnName).ttl(columnName).writeTime(columnName);
					}

					return select.from(keyspace, cfDef.getName()).where(in(partitionKeyCol, rowSliceQuery.getRowSlice().getKeys().toArray()));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			
			List<Object> values = new ArrayList<Object>();
			values.addAll(rowSliceQuery.getRowSlice().getKeys());
			return pStatement.bind(values.toArray());		
		}
	};
	
	private Where addWhereClauseForRowRange(String keyAlias, Select select, RowRange<?> rowRange) {

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

				where = select.where(gte(keyAlias, BIND_MARKER))
						.and(lte(keyAlias, BIND_MARKER));

			} else if (rowRange.getStartKey() != null) {				
				where = select.where(gte(keyAlias, BIND_MARKER));

			} else if (rowRange.getEndKey() != null) {
				where = select.where(lte(keyAlias, BIND_MARKER));
			}

		} else if (tokenIsPresent) {
			String tokenOfKey ="token(" + keyAlias + ")";

			if (rowRange.getStartToken() != null && rowRange.getEndToken() != null) {

				where = select.where(gte(tokenOfKey, BIND_MARKER))
						.and(lte(tokenOfKey, BIND_MARKER));

			} else if (rowRange.getStartToken() != null) {
				where = select.where(gte(tokenOfKey, BIND_MARKER));

			} else if (rowRange.getEndToken() != null) {
				where = select.where(lte(tokenOfKey, BIND_MARKER));
			}
		} else { 
			where = select.where();
		}

		if (rowRange.getCount() > 0) {
			// TODO: fix this
			//where.limit(rowRange.getCount());
		}
		return where; 
	}

	private void bindWhereClauseForRowRange(List<Object> values, RowRange<?> rowRange) {

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


	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectAllColumnsForRowRange = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					Select select = selectAllColumnsFromKeyspaceAndCF();
					return addWhereClauseForRowRange(partitionKeyCol, select, rowSliceQuery.getRowSlice().getRange());
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {

			List<Object> values = new ArrayList<Object>();
			bindWhereClauseForRowRange(values, rowSliceQuery.getRowSlice().getRange());
			return pStatement.bind(values.toArray(new Object[values.size()])); 
		}
	};
	
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectColumnSetForRowRange = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					Select.Selection select = QueryBuilder.select();
					select.column(partitionKeyCol);

					for (Object col : rowSliceQuery.getColumnSlice().getColumns()) {
						String columnName = (String)col;
						select.column(columnName).ttl(columnName).writeTime(columnName);
					}

					Select selection = select.from(keyspace, cfDef.getName());
					Where where = addWhereClauseForRowRange(partitionKeyCol, selection, rowSliceQuery.getRowSlice().getRange());
					return where;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			
			List<Object> values = new ArrayList<Object>();
			bindWhereClauseForRowRange(values, rowSliceQuery.getRowSlice().getRange());
			return pStatement.bind(values.toArray());
		}
	};

	public BoundStatement getQueryStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

		
		switch (rowSliceQuery.getRowQueryType()) {

		case RowKeys:
			return getRowKeysQueryStatement(rowSliceQuery, useCaching);
		case RowRange: 
			return getRowRangeQueryStatement(rowSliceQuery, useCaching);
		default :
			throw new RuntimeException("RowSliceQuery use case not supported.");
		}
	}
	

	
	public BoundStatement getRowKeysQueryStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

		switch (rowSliceQuery.getColQueryType()) {

		case AllColumns:
			return SelectAllColumnsForRowKeys.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnSet: 
			return SelectColumnSetForRowKeys.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnRange:
			throw new RuntimeException("RowSliceQuery use case not supported.");
		default :
			throw new RuntimeException("RowSliceQuery use case not supported.");
		}
	}
	
	public BoundStatement getRowRangeQueryStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

		switch (rowSliceQuery.getColQueryType()) {

		case AllColumns:
			return SelectAllColumnsForRowRange.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnSet: 
			return SelectColumnSetForRowRange.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnRange:
			throw new RuntimeException("RowSliceQuery use case not supported.");
		default :
			throw new RuntimeException("RowSliceQuery use case not supported.");
		}
	}
}
