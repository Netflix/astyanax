package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.math.BigInteger;
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
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice.RowRange;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;

/**
 * This class encapsulates all the query generators for row range queries. There are different row query
 * generators depending on the specific query signature. 
 * 
 * e.g 
 * 1. Select all columns for all the rows in the row range
 * 2. Select row ranges with column slice
 * 3. Select row ranges with column range
 * 4. Select row ranges using a composite range builder for composite column based schema
 * 
 * Note that for simplicity and brevity, there is another class that handles similar operations for queries that 
 * specify a collection of row keys as opposed to a row range. 
 * See {@link CFRowKeysQueryGen} for that implementation. The current class is meant for row range queries only.  
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
public class CFRowRangeQueryGen extends CFRowSliceQueryGen {

	/**
	 * Constructor
	 * 
	 * @param session
	 * @param keyspaceName
	 * @param cfDefinition
	 */
	public CFRowRangeQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {
		super(session, keyspaceName, cfDefinition);
	}

	/**
	 * Private helper for constructing the where clause for row ranges
	 * @param keyAlias
	 * @param select
	 * @param rowRange
	 * @return
	 */
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

	/**
	 * Private helper for constructing the bind values for the given row range. Note that the assumption here is that 
	 * we have a previously constructed prepared statement that we can bind these values with. 
	 * 
	 * @param keyAlias
	 * @param select
	 * @param rowRange
	 * @return
	 */
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

	/**
	 * Query generator for selecting all columns for the specified row range. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e all columns)
	 */
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
					
					// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES
					RowRange<?> range = rowSliceQuery.getRowSlice().getRange();
					Collection<?> cols = rowSliceQuery.getColumnSlice().getColumns();
					Object[] columns = cols.toArray(new Object[cols.size()]); 

					Select select = selectAllColumnsFromKeyspaceAndCF();

					if (columns != null && columns.length > 0) {
						select.allowFiltering();
					}
					Where where = addWhereClauseForRowRange(partitionKeyCol, select, range);
					where.and(in(clusteringKeyCols.get(0).getName(), columns));

					return where;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			
			List<Object> values = new ArrayList<Object>();

			bindWhereClauseForRowRange(values, rowSliceQuery.getRowSlice().getRange());
			values.addAll(rowSliceQuery.getColumnSlice().getColumns());

			return pStatement.bind(values.toArray());
		}
	};

	
	/**
	 * Query generator for selecting a specified column range with a specified row range. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e similar column range for the row range)
	 */
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectColumnRangeForRowRange = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					Select select = selectAllColumnsFromKeyspaceAndCF();
					
					CqlColumnSlice<?> columnSlice = rowSliceQuery.getColumnSlice();
					
					if (columnSlice != null && columnSlice.isRangeQuery()) {
						select.allowFiltering();
					}

					Where where = addWhereClauseForRowRange(partitionKeyCol, select, rowSliceQuery.getRowSlice().getRange());			
					where = addWhereClauseForColumnRange(where, columnSlice);
					return where;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {

			List<Object> values = new ArrayList<Object>();

			bindWhereClauseForRowRange(values, rowSliceQuery.getRowSlice().getRange());
			bindWhereClauseForColumnRange(values, rowSliceQuery.getColumnSlice());
			
			return pStatement.bind(values.toArray());
		}
	};
	
	/**
	 * Query generator for selecting a specified composite column range with a specified row range. 
	 * 
	 * Note that this object is an implementation of {@link QueryGenCache}
	 * and hence it maintains a cached reference to the previously constructed {@link PreparedStatement} for row range queries with the same 
	 * signature  (i.e similar composite column range for the row range)
	 */
	private QueryGenCache<CqlRowSliceQueryImpl<?,?>> SelectCompositeColumnRangeForRowRange = new QueryGenCache<CqlRowSliceQueryImpl<?,?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					Select select = selectAllColumnsFromKeyspaceAndCF();
					CompositeByteBufferRange compositeRange = rowSliceQuery.getCompositeRange();
					if (compositeRange != null) {
						select.allowFiltering();
					}

					Where where = addWhereClauseForRowRange(partitionKeyCol, select, rowSliceQuery.getRowSlice().getRange());	
					where = addWhereClauseForCompositeColumnRange(where, compositeRange);
					return where;
				}
				
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {

			List<Object> values = new ArrayList<Object>();

			bindWhereClauseForRowRange(values, rowSliceQuery.getRowSlice().getRange());
			bindWhereClauseForCompositeColumnRange(values, rowSliceQuery.getCompositeRange());

			return pStatement.bind(values.toArray());
		}
	};
	
	/**
	 * Main method used to generate the query for the specified row slice query. 
	 * Note that depending on the query signature, the caller may choose to enable/disable caching
	 * 
	 * @param rowSliceQuery: The Astaynax query for which we need to generate a java driver query
	 * @param useCaching: boolean condition indicating whether we should use a previously cached prepared stmt or not. 
	 *                    If false, then the cache is ignored and we generate the prepared stmt for this query
	 *                    If true, then the cached prepared stmt is used. If the cache has not been inited, 
	 *                    then the prepared stmt is constructed for this query and subsequently cached
	 *                    
	 * @return BoundStatement: they statement for this Astyanax query
	 */
	public BoundStatement getQueryStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

		switch (rowSliceQuery.getColQueryType()) {

		case AllColumns:
			return SelectAllColumnsForRowRange.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnSet: 
			return SelectColumnSetForRowRange.getBoundStatement(rowSliceQuery, useCaching);
		case ColumnRange:
			if (isCompositeColumn) {
				return SelectCompositeColumnRangeForRowRange.getBoundStatement(rowSliceQuery, useCaching);
			} else {
				return SelectColumnRangeForRowRange.getBoundStatement(rowSliceQuery, useCaching);
			}
		default :
			throw new RuntimeException("RowSliceQuery with row range use case not supported.");
		}
	}
}
