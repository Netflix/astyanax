package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

public class CqlColumnQueryImpl<C> implements ColumnQuery<C> {

	private ChainedContext context; 
	
	CqlColumnQueryImpl(ChainedContext ctx) {
		this.context = ctx;
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

		private final Object rowKey = context.getNext(Object.class); 
		private final Object column = context.getNext(Object.class); 

		public InternalColumnQueryExecutionImpl() {
			super(context);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN;
		}

		@Override
		public Query getQuery() {
			
			boolean isCompositeType = cf.getColumnSerializer().getComparatorType() == ComparatorType.COMPOSITETYPE;
			
			Query query = null; 
			if (CqlFamilyFactory.OldStyleThriftMode()) {
			
				if (!isCompositeType) {
					query = QueryBuilder.select("column1", "value")
										.from(keyspace, cf.getName())
										.where(eq("key", rowKey))
										.and(eq("column1", column));
				} else {
					
					/**  COMPOSITE COLUMN QUERY */
					AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) this.cf.getColumnSerializer();
					List<ComponentSerializer<?>> components = compSerializer.getComponents();
					
					// select the individual columns as dictated by the no of component serializers
					Selection select = QueryBuilder.select();
					
					for (int index = 1; index <= components.size(); index++) {
						select.column("column" + index);
					}
					
					select.column("value");
					
					Where where = select.from(keyspace, cf.getName()).where(eq("key", rowKey));
					
					int index = 1;
					for (ComponentSerializer<?> component : components) {
						where.and(eq("column" + index++, component.getFieldValueDirectly(column)));
					}

					return where;
				}
				
			} else {
				
				if (isCompositeType) {
					throw new NotImplementedException();
				}
				query = QueryBuilder.select(String.valueOf(column))
						.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));
			}
			return query;
		}

		@Override
		public Column<C> parseResultSet(ResultSet rs) {
			
			Row row = rs.one();
			if (row == null) {
				return new CqlColumnImpl();
			}
			
			Object columnName = CqlTypeMapping.getDynamicColumn(row, cf.getColumnSerializer());
			CqlColumnImpl<C> cqlCol = new CqlColumnImpl<C>((C) columnName, row, row.getColumnDefinitions().size()-1);
			return cqlCol;
		}
	}
}
