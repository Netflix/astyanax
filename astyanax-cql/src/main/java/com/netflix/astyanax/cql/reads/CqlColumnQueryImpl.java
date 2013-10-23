package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.List;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
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
	private final List<ColumnDefinition> pkCols;
	private final String keyColumnAlias;

	CqlColumnQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<?,C> cfCtx, Object rowKey, C colName) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowKey = rowKey;
		this.columnName = colName;
		
		ColumnFamily<?,?> cf = cfCtx.getColumnFamily();
		cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		pkCols = cfDef.getPartitionKeyColumnDefinitionList();
		keyColumnAlias = pkCols.get(0).getName();
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

		public InternalColumnQueryExecutionImpl() {
			super(ksContext, cfContext);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN;
		}

		@Override
		public Query getQuery() {

			boolean isCompositeType = cf.getColumnSerializer().getComparatorType() == ComparatorType.COMPOSITETYPE;

			if (!isCompositeType) {

				if (pkCols.size() == 1) {

					return QueryBuilder.select((String)columnName)
							.from(keyspace, cf.getName())
							.where(eq(keyColumnAlias, rowKey));
				} else {

					String pkColName = pkCols.get(1).getName();
					List<ColumnDefinition> valDef = cfDef.getValueColumnDefinitionList();
					String valueColName = valDef.get(0).getName();

					return QueryBuilder.select(valueColName)
							.from(keyspace, cf.getName())
							.where(eq(keyColumnAlias, rowKey))
							.and(eq(pkColName, columnName));

				}
			} else {

				/**  COMPOSITE COLUMN QUERY */

				List<ColumnDefinition> valDef = cfDef.getValueColumnDefinitionList();
				String valueColName = valDef.get(0).getName();

				AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) this.cf.getColumnSerializer();
				List<ComponentSerializer<?>> components = compSerializer.getComponents();

				// select the individual columns as dictated by the no of component serializers
				Builder select = QueryBuilder.select(valueColName);

				Where where = select.from(keyspace, cf.getName()).where(eq(keyColumnAlias, rowKey));

				int index = 1;
				for (ComponentSerializer<?> component : components) {
					where.and(eq(pkCols.get(index).getName(), component.getFieldValueDirectly(columnName)));
					index++;
				}

				return where;
			}
		}

		@Override
		public Column<C> parseResultSet(ResultSet rs) {

			Row row = rs.one();
			if (row == null) {
				return new CqlColumnImpl<C>();
			}

			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) columnName, row, 0);
			return cqlCol;
		}
	}
}
