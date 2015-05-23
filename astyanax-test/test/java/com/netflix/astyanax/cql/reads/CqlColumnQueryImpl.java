package com.netflix.astyanax.cql.reads;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.reads.model.CqlColumnImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnQuery;

/**
 * Impl for the {@link ColumnQuery} interface using the java driver. 
 * This class is responsible for selecting a single column for the specified row key. 
 * 
 * Note that this class acts like a placeholder for all the query context, but does not construct the query itself. 
 * For details on how the query is actually constructed see {@link CFColumnQueryGen}
 * 
 * @author poberai
 *
 * @param <C>
 */
public class CqlColumnQueryImpl<C> implements ColumnQuery<C> {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<?,C> cfContext;
	private final Object rowKey;
	private final C columnName;

	private boolean useCaching = false; 
	
	private final CqlColumnFamilyDefinitionImpl cfDef;

	CqlColumnQueryImpl(KeyspaceContext ksCtx, CFQueryContext<?,C> cfCtx, Object rowKey, C colName, boolean caching) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowKey = rowKey;
		this.columnName = colName;
		this.useCaching = caching;
		
		ColumnFamily<?,?> cf = cfCtx.getColumnFamily();
		cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
	}

	@Override
	public OperationResult<Column<C>> execute() throws ConnectionException {
		return new InternalColumnQueryExecutionImpl(this).execute();
	}

	@Override
	public ListenableFuture<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
		return new InternalColumnQueryExecutionImpl(this).executeAsync();
	}

	private class InternalColumnQueryExecutionImpl extends CqlAbstractExecutionImpl<Column<C>> {

		private final CqlColumnQueryImpl<?> columnQuery; 
		
		public InternalColumnQueryExecutionImpl(CqlColumnQueryImpl<?> query) {
			super(ksContext, cfContext);
			this.columnQuery = query;
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN;
		}

		@Override
		public Statement getQuery() {
			return cfDef.getRowQueryGenerator().getQueryStatement(columnQuery, useCaching);
		}

		@Override
		public Column<C> parseResultSet(ResultSet rs) throws NotFoundException {

			Row row = rs.one();
			if (row == null) {
				return null;
			}

			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) columnName, row, 0);
			return cqlCol;
		}
	}

	public Object getRowKey() {
		return rowKey;
	}
	
	public C getColumnName() {
		return columnName;
	}
	
	public ColumnFamily<?,C> getCF() {
		return this.cfContext.getColumnFamily();
	}
}
