package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.in;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;

/**
 * This class encapsulates all the query generators for row slice queries that use a collection of row keys. 
 * There are different row query generators depending on the specific query signature. 
 * 
 * e.g 
 * 1. Select all columns for all the rows in the row range
 * 2. Select rows with column slice
 * 3. Select rows with column range
 * 4. Select rows using a composite range builder for composite column based schema
 * 
 * Note that for simplicity and brevity, there is another class that handles similar operations for queries that 
 * specify a row range as opposed to a collection of row keys (as is done here). 
 * See {@link CFRowRangeQueryGen} for that implementation. The current class is meant for row slice queries using only row key collections.  
 * 
 * Each of the query generators uses the {@link QueryGenCache} so that it can cache the {@link PreparedStatement} as well
 * for future use by queries with the same signatures.
 * 
 * But one must use this with care, since the subsequent query must have the exact signature, else binding values with 
 * the previously constructed prepared statement will break. 
 * 
 * Here is a simple example of a bad query that is not cacheable. 
 * 
 * Say that we want a simple query with a column range in it. 
 * 
 *     ks.prepareQuery(myCF)
 *       .getRow("1")
 *       .withColumnSlice("colStart")
 *       .execute();
 *       
 *     In most cases this query lends itself to a CQL3 representation as follows
 *     
 *     SELECT * FROM ks.mfCF WHERE KEY = ? AND COLUMN1 > ?;
 *     
 * Now say that we want  to perform a successive query (with caching turned ON), but add to the column range query
 *    
 *     ks.prepareQuery(myCF)
 *       .getRow("1")
 *       .withColumnSlice("colStart", "colEnd")
 *       .execute();
 *       
 *     NOTE THE USE OF BOTH colStart AND colEnd     <----- THIS IS A DIFFERENT QUERY SIGNATURE
 *     AND THE CQL QUERY WILL PROBABLY LOOK LIKE 
 *     
 *     SELECT * FROM ks.mfCF WHERE KEY = ? AND COLUMN1 > ?  AND COLUMN1 < ?;     <----- NOTE THE EXTRA BIND MARKER AT THE END FOR THE colEnd
 * 
 * If we re-use the previously cached prepared statement, then it will not work for the new query signature. The way out of this is to NOT
 * use caching with different query signatures. 
 * 
 * @author poberai
 *
 */
public class CFRowKeysQueryGen extends CFRowSliceQueryGen {

	public CFRowKeysQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {
		super(session, keyspaceName, cfDefinition);
	}

	/**
	 * Query generator for selecting all columns for the specified row keys. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e all columns for a similar set of row keys)
	 */
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectAllColumnsForRowKeys = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					
					Select select = selectAllColumnsFromKeyspaceAndCF();
					return select.where(in(partitionKeyCol, bindMarkerArray(rowSliceQuery.getRowSlice().getKeys().size())));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return pStatement.bind(rowSliceQuery.getRowSlice().getKeys().toArray());
		}
	};
	
	/**
	 * Query generator for selecting a column set for the specified row keys. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e a similar set of columns for a similar set of rows )
	 */
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectColumnSetForRowKeys = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {
	
		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					if (clusteringKeyCols.size() != 1) {
						throw new RuntimeException("Cannot perform row slice with col slice query for this schema, clusteringKeyCols.size(): " 
								+ clusteringKeyCols.size());
					}
						
					Collection<?> rowKeys = rowSliceQuery.getRowSlice().getKeys();
					Collection<?> cols = rowSliceQuery.getColumnSlice().getColumns();

					// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES
					Object[] columns = cols.toArray(new Object[cols.size()]); 

					String clusteringCol = clusteringKeyCols.get(0).getName();

					Select select = selectAllColumnsFromKeyspaceAndCF();
					return select.where(in(partitionKeyCol, bindMarkerArray(rowKeys.size())))
							.and(in(clusteringCol, bindMarkerArray(columns.length)));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {

			if (clusteringKeyCols.size() != 1) {
				throw new RuntimeException("Cannot perform row slice with col slice query for this schema, clusteringKeyCols.size(): " 
						+ clusteringKeyCols.size());
			}
			
			List<Object> values = new ArrayList<Object>();
			values.addAll(rowSliceQuery.getRowSlice().getKeys());
			values.addAll(rowSliceQuery.getColumnSlice().getColumns());

			return pStatement.bind(values.toArray());		
		}
	};
	
	/**
	 * Query generator for selecting a column range for the specified row keys. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e a similar column range for a similar set of rows)
	 */
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectColumnRangeForRowKeys = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					if (clusteringKeyCols.size() != 1) {
						throw new RuntimeException("Cannot perform row slice with col slice query for this schema, clusteringKeyCols.size(): " 
								+ clusteringKeyCols.size());
					}
						
					Select select = selectAllColumnsFromKeyspaceAndCF();
					Where where = select.where(in(partitionKeyCol, bindMarkerArray(rowSliceQuery.getRowSlice().getKeys().size())));
					where = addWhereClauseForColumnRange(where, rowSliceQuery.getColumnSlice());
					return where;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {

			if (clusteringKeyCols.size() != 1) {
				throw new RuntimeException("Cannot perform row slice with col slice query for this schema, clusteringKeyCols.size(): " 
						+ clusteringKeyCols.size());
			}
			
			List<Object> values = new ArrayList<Object>();

			values.addAll(rowSliceQuery.getRowSlice().getKeys());
			bindWhereClauseForColumnRange(values, rowSliceQuery.getColumnSlice());

			return pStatement.bind(values.toArray());
		}
	};
	
	
	/**
	 * Query generator for selecting a composite column range for the specified row keys. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e a similar composite column range for a similar set of rows)
	 */
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectCompositeColumnRangeForRowKeys = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					Select select = selectAllColumnsFromKeyspaceAndCF();
					Where stmt = select.where(in(partitionKeyCol, bindMarkerArray(rowSliceQuery.getRowSlice().getKeys().size())));
					stmt = addWhereClauseForCompositeColumnRange(stmt, rowSliceQuery.getCompositeRange());
					return stmt;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {

			List<Object> values = new ArrayList<Object>();

			values.addAll(rowSliceQuery.getRowSlice().getKeys());
			bindWhereClauseForCompositeColumnRange(values, rowSliceQuery.getCompositeRange());

			return pStatement.bind(values.toArray());
		}
	};
	
	/**
	 * Main method that is used to generate the java driver statement from the given Astyanax row slice query. 
	 * Note that the method allows the caller to specify whether to use caching or not. 
	 * 
	 * If caching is disabled, then the PreparedStatement is generated every time
	 * If caching is enabled, then the cached PreparedStatement is used for the given Astyanax RowSliceQuery. 
	 * In this case if the PreparedStatement is missing, then it is constructed from the Astyanax query and 
	 * used to init the cached reference and hence can be used by other subsequent Astayanx RowSliceQuery
	 * operations with the same signature (that opt in for  caching)
	 * 
	 * @param rowSliceQuery
	 * @param useCaching
	 * @return
	 */
	public BoundStatement getQueryStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

		switch (rowSliceQuery.getColQueryType()) {

		case AllColumns:
			return SelectAllColumnsForRowKeys.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnSet: 
			return SelectColumnSetForRowKeys.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnRange:
			if (isCompositeColumn) {
				return SelectCompositeColumnRangeForRowKeys.getBoundStatement(rowSliceQuery, useCaching);
			} else {
				return SelectColumnRangeForRowKeys.getBoundStatement(rowSliceQuery, useCaching);
			}
		default :
			throw new RuntimeException("RowSliceQuery with row keys use case not supported.");
		}
	}
}
