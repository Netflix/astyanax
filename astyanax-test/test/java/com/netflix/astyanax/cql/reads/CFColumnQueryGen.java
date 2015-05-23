package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;

public class CFColumnQueryGen {

	private AtomicReference<Session> sessionRef = new AtomicReference<Session>(null);
	private final String keyspace; 
	private final CqlColumnFamilyDefinitionImpl cfDef;

	private final String partitionKeyCol;
	private final List<ColumnDefinition> clusteringKeyCols;
	private final List<ColumnDefinition> regularCols;
	
	private boolean isCompositeColumn = false;
	private boolean isFlatTable = false;
	
	private static final String BIND_MARKER = "?";

	

	public CFColumnQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.sessionRef.set(session);
		
		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		regularCols = cfDef.getRegularColumnDefinitionList();
		
		isCompositeColumn = (clusteringKeyCols.size() > 1);
		isFlatTable = (clusteringKeyCols.size() == 0);
	}
	
	private QueryGenCache<CqlColumnQueryImpl<?>> ColumnQueryWithClusteringKey = new QueryGenCache<CqlColumnQueryImpl<?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlColumnQueryImpl<?> columnQuery) {
			
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					if (clusteringKeyCols.size() != 1) {
						throw new RuntimeException("Cannot use this query for this schema, clustetingKeyCols.size: " + clusteringKeyCols.size());
					}
					
					String valueColName = regularCols.get(0).getName();

					return QueryBuilder.select()
							.column(valueColName).ttl(valueColName).writeTime(valueColName)
							.from(keyspace, cfDef.getName())
							.where(eq(partitionKeyCol, BIND_MARKER))
							.and(eq(clusteringKeyCols.get(0).getName(), BIND_MARKER));
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnQueryImpl<?> columnQuery) {
			return pStatement.bind(columnQuery.getRowKey(), columnQuery.getColumnName());		
		}
	};
	
	private QueryGenCache<CqlColumnQueryImpl<?>> ColumnQueryWithCompositeColumn = new QueryGenCache<CqlColumnQueryImpl<?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlColumnQueryImpl<?> columnQuery) {
			
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					
					if (clusteringKeyCols.size() <= 1) {
						throw new RuntimeException("Cannot use this query for this schema, clustetingKeyCols.size: " + clusteringKeyCols.size());
					}
					
					String valueColName = regularCols.get(0).getName();

					ColumnFamily<?,?> cf = columnQuery.getCF();
					AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
					List<ComponentSerializer<?>> components = compSerializer.getComponents();

					// select the individual columns as dictated by the no of component serializers
					Builder select = QueryBuilder.select()
							.column(valueColName).ttl(valueColName).writeTime(valueColName);

					Where where = select.from(keyspace, cfDef.getName()).where(eq(partitionKeyCol, BIND_MARKER));

					for (int index = 0; index<components.size(); index++) {
						where.and(eq(clusteringKeyCols.get(index).getName(), BIND_MARKER));
					}

					return where;
				}
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnQueryImpl<?> columnQuery) {
		
			List<Object> values = new ArrayList<Object>();
			values.add(columnQuery.getRowKey());
			
			ColumnFamily<?,?> cf = columnQuery.getCF();
			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
			List<ComponentSerializer<?>> components = compSerializer.getComponents();

			Object columnName = columnQuery.getColumnName();
			for (ComponentSerializer<?> component : components) {
				values.add(component.getFieldValueDirectly(columnName));
			}

			return pStatement.bind(values.toArray());
		}
	};
	
	
	private QueryGenCache<CqlColumnQueryImpl<?>> FlatTableColumnQuery = new QueryGenCache<CqlColumnQueryImpl<?>>(sessionRef) {

		@Override
		public Callable<RegularStatement> getQueryGen(final CqlColumnQueryImpl<?> columnQuery) {
			
			return new Callable<RegularStatement>() {

				@Override
				public RegularStatement call() throws Exception {
					
					if (clusteringKeyCols.size() != 0) {
						throw new RuntimeException("Cannot use this query for this schema, clustetingKeyCols.size: " + clusteringKeyCols.size());
					}

					String columnNameString = (String)columnQuery.getColumnName();
					return QueryBuilder.select()
							.column(columnNameString).ttl(columnNameString).writeTime(columnNameString)
							.from(keyspace, cfDef.getName())
							.where(eq(partitionKeyCol, BIND_MARKER));				}
				
			};
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnQueryImpl<?> columnQuery) {
			return pStatement.bind(columnQuery.getRowKey());
		}
	};
	
	public BoundStatement getQueryStatement(CqlColumnQueryImpl<?> columnQuery, boolean useCaching) {

		if (isFlatTable) {
			return FlatTableColumnQuery.getBoundStatement(columnQuery, useCaching);
		}
		
		if (isCompositeColumn) {
			return ColumnQueryWithCompositeColumn.getBoundStatement(columnQuery, useCaching);
		} else {
			return ColumnQueryWithClusteringKey.getBoundStatement(columnQuery, useCaching);
		}
	}
}
