package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.direct.DirectCqlPreparedStatement;
import com.netflix.astyanax.cql.reads.model.CqlColumnImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

public class CqlColumnQueryImpl<C> implements ColumnQuery<C> {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<?,C> cfContext;
	private final Object rowKey;
	private final C columnName;

	private final CqlColumnFamilyDefinitionImpl cfDef;

	private DirectCqlPreparedStatement preparedStatement = null; 
	
	CqlColumnQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<?,C> cfCtx, Object rowKey, C colName) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowKey = rowKey;
		this.columnName = colName;
		
		ColumnFamily<?,?> cf = cfCtx.getColumnFamily();
		cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
	}

	@Override
	public OperationResult<Column<C>> execute() throws ConnectionException {
		return new InternalColumnQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
		return new InternalColumnQueryExecutionImpl().executeAsync();
	}

	private class InternalColumnQueryExecutionImpl extends CqlAbstractExecutionImpl<Column<C>> {

		String partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		List<ColumnDefinition> clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		List<ColumnDefinition> regularCols = cfDef.getRegularColumnDefinitionList();

		public InternalColumnQueryExecutionImpl() {
			super(ksContext, cfContext);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN;
		}

		@Override
		public Statement getQuery() {

			boolean isCompositeType = cf.getColumnSerializer().getComparatorType() == ComparatorType.COMPOSITETYPE;
			boolean isPreparedStatementProvided = preparedStatement != null;
			
			if (!isCompositeType) {
				return isPreparedStatementProvided ? bindNonCompositeColumn() : selectNonCompositeColumn();
			} else {
				return isPreparedStatementProvided ? bindCompositeColumn() : selectCompositeColumn();
			}
		}
			
		private Statement selectNonCompositeColumn() {

			if (clusteringKeyCols.size() == 0) {

				String columnNameString = (String)columnName;
				return QueryBuilder.select()
						.column(columnNameString).ttl(columnNameString).writeTime(columnNameString)
						.from(keyspace, cf.getName())
						.where(eq(partitionKeyCol, rowKey));
			} else {

				String valueColName = regularCols.get(0).getName();

				return QueryBuilder.select()
						.column(valueColName).ttl(valueColName).writeTime(valueColName)
						.from(keyspace, cf.getName())
						.where(eq(partitionKeyCol, rowKey))
						.and(eq(clusteringKeyCols.get(0).getName(), columnName));

			}
		}

		private Statement bindNonCompositeColumn() {

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			if (clusteringKeyCols.size() == 0) {
				return pStatement.bind(rowKey);
			} else {
				return pStatement.bind(rowKey, columnName);
			}
		}

		/**  COMPOSITE COLUMN QUERY */
		private Statement selectCompositeColumn() {
			String valueColName = regularCols.get(0).getName();

			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) this.cf.getColumnSerializer();
			List<ComponentSerializer<?>> components = compSerializer.getComponents();

			// select the individual columns as dictated by the no of component serializers
			Builder select = QueryBuilder.select()
					.column(valueColName).ttl(valueColName).writeTime(valueColName);

			Where where = select.from(keyspace, cf.getName()).where(eq(partitionKeyCol, rowKey));

			int index = 0;
			for (ComponentSerializer<?> component : components) {
				where.and(eq(clusteringKeyCols.get(index).getName(), component.getFieldValueDirectly(columnName)));
				index++;
			}

			return where;
		}
		
		private Statement bindCompositeColumn() {
			
			List<Object> values = new ArrayList<Object>();
			values.add(rowKey);
			
			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) this.cf.getColumnSerializer();
			List<ComponentSerializer<?>> components = compSerializer.getComponents();

			for (ComponentSerializer<?> component : components) {
				values.add(component.getFieldValueDirectly(columnName));
			}

			PreparedStatement pStatement = preparedStatement.getInnerPreparedStatement();
			return pStatement.bind(values.toArray());
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

	@Override
	public ColumnQuery<C> withPreparedStatement(CqlPreparedStatement preparedStatement) {
		this.preparedStatement = (DirectCqlPreparedStatement) preparedStatement;
		return this;
	}

	@Override
	public CqlPreparedStatement asPreparedStatement() {
		this.preparedStatement = null;
		RegularStatement stmt = (RegularStatement) new InternalColumnQueryExecutionImpl().getQuery();
		Session session = ksContext.getSession();
		PreparedStatement pStmt = session.prepare(stmt.getQueryString());
		this.preparedStatement = new DirectCqlPreparedStatement(session, pStmt);
		return this.preparedStatement;
	}
}
