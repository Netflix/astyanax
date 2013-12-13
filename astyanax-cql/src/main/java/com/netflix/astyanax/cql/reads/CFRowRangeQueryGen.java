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

public class CFRowRangeQueryGen extends CFRowSliceQueryGen {

	public CFRowRangeQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {
		super(session, keyspaceName, cfDefinition);
	}

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


	private RowSliceQueryCache SelectAllColumnsForRowRange = new RowSliceQueryCache() {

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
	
	private RowSliceQueryCache SelectColumnSetForRowRange = new RowSliceQueryCache() {

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

	private RowSliceQueryCache SelectColumnRangeForRowRange = new RowSliceQueryCache() {

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
	
	private RowSliceQueryCache SelectCompositeColumnRangeForRowRange = new RowSliceQueryCache() {

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
