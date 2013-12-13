package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.List;
import java.util.concurrent.Callable;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;

public class FlatTableRowQueryGen {
	
	private Session session;
	private final String keyspace; 
	private final CqlColumnFamilyDefinitionImpl cfDef;

	private final String partitionKeyCol;
	private final String[] allPrimayKeyCols;
	private final List<ColumnDefinition> regularCols;
	
	private static final String BIND_MARKER = "?";


	public FlatTableRowQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.session = session;

		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		allPrimayKeyCols = cfDef.getAllPkColNames();
		regularCols = cfDef.getRegularColumnDefinitionList();
	}
	
	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectEntireRow = new QueryGenCache<CqlRowQueryImpl<?,?>>(session) {

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

	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectColumnSlice = new QueryGenCache<CqlRowQueryImpl<?,?>>(session) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowQueryImpl<?, ?> rowQuery) {

			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {

					Select.Selection select = QueryBuilder.select();
					select.column(partitionKeyCol);

					for (Object col : rowQuery.getColumnSlice().getColumns()) {
						String columnName = (String)col;
						select.column(columnName).ttl(columnName).writeTime(columnName);
					}

					return select.from(keyspace, cfDef.getName()).where(eq(partitionKeyCol, BIND_MARKER));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowQueryImpl<?, ?> rowQuery) {
			return pStatement.bind(rowQuery.getRowKey());
		}
	};

	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectColumnRange = new QueryGenCache<CqlRowQueryImpl<?,?>>(session) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlRowQueryImpl<?, ?> rowQuery) {
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
				}				
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlRowQueryImpl<?, ?> rowQuery) {
			throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
		}
	};
	
	public Statement getQueryStatement(final CqlRowQueryImpl<?,?> rowQuery, boolean useCaching)  {
		
		switch (rowQuery.getQueryType()) {
		
		case AllColumns:
			return SelectEntireRow.getBoundStatement(rowQuery, useCaching);
		case ColumnSlice:
			return SelectColumnSlice.getBoundStatement(rowQuery, useCaching);
		case ColumnRange:
			return SelectColumnRange.getBoundStatement(rowQuery, useCaching);
		default :
			throw new RuntimeException("RowQuery use case not supported. Fix this!!");
		}
	}


}
