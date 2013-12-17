package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

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

/**
 * Read query generator for queries on flat tables i.e tables with no clustering keys.
 * 
 * The class lives along other implementations like {@link CFRowQueryGen}, {@link CFRowRangeQueryGen} and {@link CFRowKeysQueryGen}
 * The structure of queries for flat tables was different enough that they warranted their own class. If your schema contains clustering keys
 * then see {@link CFRowQueryGen}, {@link CFRowRangeQueryGen} and {@link CFRowKeysQueryGen} for implementation details. 
 * 
 * Note that the class manages several individual query generators for different use cases like 
 * 1. Selecting the entire row
 * 2. Performing a column slice operation i.e column collection
 *  
 * Each of these query generators uses the {@link QueryGenCache} to maintain a cached reference to the {@link PreparedStatement}
 * that it creates, which can then be leveraged by subsequent flat table queries that have the same signature. 
 * 
 * Note the one must use caching for flat table queries with EXTREME CAUTION. The cacheability of a query depends on the actual 
 * signature of a query. If you use different queries with different signatures for the same column slice operations, then caching will 
 * not work. Here is an example where caching will break queries.
 * 
 *  Consider a query where you want to perform a column slice operation i.e cherry pick some column for a given row. 
 *  The Astyanax query for that will look somewhat like this 
 *  
 *         ks.prepareQuery( myCF )
 *           .getRow( 1 )
 *           .getColumn( first_name )
 *           .execute(); 
 *           
 *  Now if the table is a flat table, then the query for this will look something like 
 *  
 *       SELECT first_name FROM ks.myCF WHERE key = ? ;
 *       
 *  Note the bind marker for the row key. That is the only parameter here which is dynamic and the column name here i.e "first_name" is not
 *  and hence is part of the signature of this query. 
 *  
 *  Now if we were to attempt to re-use the same prepared statement for a query like this 
 *  
 *         ks.prepareQuery( myCF )
 *           .getRow( 1 )
 *           .getColumn( last_name )  <------ NOTE THAT WE ARE CHANGING OUR COLUMN SLICE AND HENCE VIOLATING THE QUERY SIGNATURE
 *           .execute(); 
 *  
 *  Then this will break since the CQL query required for this is 
 *  
 *       SELECT first_name FROM ks.myCF WHERE key = ? ;
 *       
 *   In cases like this, DO NOT use statement caching. 
 *  
 * @author poberai
 *
 */
public class FlatTableRowQueryGen {
	
	// Reference to the session that is needed for "preparing" the statements
	private AtomicReference<Session> sessionRef = new AtomicReference<Session>(null);
	private final String keyspace; 
	private final CqlColumnFamilyDefinitionImpl cfDef;

	private final String partitionKeyCol;
	private final String[] allPrimayKeyCols;
	private final List<ColumnDefinition> regularCols;
	
	private static final String BIND_MARKER = "?";

	/**
	 * Constructor
	 * @param session
	 * @param keyspaceName
	 * @param cfDefinition
	 */
	public FlatTableRowQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.sessionRef.set(session);
		
		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		allPrimayKeyCols = cfDef.getAllPkColNames();
		regularCols = cfDef.getRegularColumnDefinitionList();
	}
	
	/**
	 * Query generator that generates a query to read the entire row, i.e all the columns. 
	 * Note that since it implements the {@link QueryGenCache} it also maintains an inner cached reference 
	 * to the {@link PreparedStatement} that it creates which can then be re-used by subsequent queries that 
	 * have the same signature (i.e read all columns)
	 */
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

	/**
	 * Query generator that generates a query to peform a column slice operation on the specified row. 
	 * Note that performing column slice operations on flat tables is dangerous since the query signature is not the same,
	 * hence use this with caution. See above for an explanation on query signatures and query cacheability. 
	 * 
	 * Note that since it implements the {@link QueryGenCache} it also maintains an inner cached reference 
	 * to the {@link PreparedStatement} that it creates which can then be re-used by subsequent queries that 
	 * have the same signature (i.e read the same column slice for a given row)
	 */
	private QueryGenCache<CqlRowQueryImpl<?,?>> SelectColumnSlice = new QueryGenCache<CqlRowQueryImpl<?,?>>(sessionRef) {

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
	
	public Statement getQueryStatement(final CqlRowQueryImpl<?,?> rowQuery, boolean useCaching)  {
		
		switch (rowQuery.getQueryType()) {
		
		case AllColumns:
			return SelectEntireRow.getBoundStatement(rowQuery, useCaching);
		case ColumnSlice:
			return SelectColumnSlice.getBoundStatement(rowQuery, useCaching);
		case ColumnRange:
			throw new RuntimeException("Cannot perform col range query with current schema, missing pk cols");
		default :
			throw new RuntimeException("Flat table RowQuery use case not supported. Fix this!!");
		}
	}
}
