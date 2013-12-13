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

public class CFRowKeysQueryGen extends CFRowSliceQueryGen {

	public CFRowKeysQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {
		super(session, keyspaceName, cfDefinition);
	}

	private RowSliceQueryCache SelectAllColumnsForRowKeys = new RowSliceQueryCache() {

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
	
	private RowSliceQueryCache SelectColumnSetForRowKeys = new RowSliceQueryCache() {

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
					return select.where(in(partitionKeyCol, rowKeys.toArray()))
							.and(in(clusteringCol, columns));
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
	
	private RowSliceQueryCache SelectColumnRangeForRowKeys = new RowSliceQueryCache() {

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
					Where where = select.where(in(partitionKeyCol, rowSliceQuery.getRowSlice().getKeys().toArray()));
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
	
	
	private RowSliceQueryCache SelectCompositeColumnRangeForRowKeys = new RowSliceQueryCache() {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowSliceQueryImpl<?, ?> rowSliceQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					Select select = selectAllColumnsFromKeyspaceAndCF();
					Where stmt = select.where(in(partitionKeyCol, rowSliceQuery.getRowSlice().getKeys().toArray()));
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
