package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

public class CFMutationQueryGenerator extends CqlStyleMutationQuery {

	private final String keyspace;
	private final ColumnFamily<?,?> cf;
	private final CqlColumnFamilyDefinitionImpl cfDef;
	private boolean isCompositeKey = false;
	private final Object rowKey;

	public CFMutationQueryGenerator(KeyspaceContext ksContext, ColumnFamilyMutationContext<?,?> cfContext, 
								   List<CqlColumnMutationImpl<?,?>> mutationList, AtomicReference<Boolean> deleteRow, 
								   AtomicReference<Integer> ttl, AtomicReference<Long> timestamp, ConsistencyLevel consistencyLevel) {
		super(ksContext, cfContext, mutationList, deleteRow, ttl, timestamp, consistencyLevel);
		
		keyspace = ksContext.getKeyspace();
		cf = cfContext.getColumnFamily();
		cfDef = (CqlColumnFamilyDefinitionImpl)cf.getColumnFamilyDefinition();
		isCompositeKey = cfDef.getClusteringKeyColumnDefinitionList().size() > 0;
		rowKey = cfContext.getRowKey();
	}

	public List<Object> getBindValues() {
		
		List<Object> values = new ArrayList<Object>();
		
		if (deleteRow.get()) {
			Preconditions.checkArgument(mutationList.size() == 0, "Cannot delete row with pending column mutations");
			values.add(super.cfContext.getRowKey());
			return values;
		}

		for (CqlColumnMutationImpl<?,?> colMutation : mutationList) {
			appendBindValues(values, colMutation);
		}

		return values;
	}
	
	public void appendBindValues(List<Object> values, CqlColumnMutationImpl<?,?> colMutation) {
		Object rowKey = super.cfContext.getRowKey();
		
		if (colMutation.deleteColumn) {

			values.add(colMutation.columnName);
			values.add(rowKey);

		} else if (colMutation.counterColumn) {

			Integer delta = (Integer) colMutation.columnValue;
			if (delta < 0) {
				delta = Math.abs(delta);
			}
			
			values.add(colMutation.columnName);
			values.add(delta);
			values.add(rowKey);
		} else {
			values.add(colMutation.columnValue);
			values.add(rowKey);
			values.add(colMutation.columnName);
		}
	}
	
	public BatchedStatements getQuery() {
		
		BatchedStatements statements = new BatchedStatements();
		
		if (deleteRow.get()) {
			Preconditions.checkArgument(mutationList.size() == 0, "Cannot delete row with pending column mutations");
			statements.addBatch(super.getDeleteEntireRowQuery(), super.cfContext.getRowKey());
			return statements;
		}

		for (CqlColumnMutationImpl<?,?> colMutation : mutationList) {
			appendQuery(statements, colMutation);
		}

		return statements;
	}
	
	public void appendQuery(BatchedStatements statements, CqlColumnMutationImpl<?,?> colMutation) {

		if (colMutation.deleteColumn) {

			statements.addBatchQuery(getDeleteColumnStatement(colMutation));
			statements.addBatchValues(getDeleteStatementBatchValues(colMutation));

		} else if (colMutation.counterColumn) {

			Long delta = (Long) colMutation.columnValue;
			boolean increment = true;
			if (delta < 0) {
				increment = false;
				delta = Math.abs(delta);
			}
			statements.addBatchQuery(getCounterColumnStatement(keyspace, cf.getName(), increment, colMutation));
			statements.addBatchValues(getBatchValues(colMutation.columnName, delta, rowKey));
		} else {
			statements.addBatchQuery(getUpdateColumnStatement(colMutation));
			statements.addBatchValues(getUpdateStatementBatchValues(colMutation));
		}
	}
	
	
	private String getUpdateColumnStatement(CqlColumnMutationImpl<?,?> colMutation) {

		//insert into standard2 (key, column1, value) values ('a', '2' , 'a2') using ttl 86400;
		
		int columnCount = 0; 
		
		StringBuilder sb = new StringBuilder("INSERT INTO ");
		sb.append(keyspace + "." + cf.getName());
		sb.append(" (");
		
		Iterator<ColumnDefinition> iter = cfDef.getPartitionKeyColumnDefinitionList().iterator();
		while (iter.hasNext()) {
			sb.append(iter.next().getName());
			columnCount++;
			if (iter.hasNext()) {
				sb.append(",");
			}
		}
		
		iter = cfDef.getClusteringKeyColumnDefinitionList().iterator();
		if (iter.hasNext()) {
			sb.append(",");
			while (iter.hasNext()) {
				sb.append(iter.next().getName());
				columnCount++;
				if (iter.hasNext()) {
					sb.append(",");
				}
			}
		}
		
		iter = cfDef.getRegularColumnDefinitionList().iterator();
		if (iter.hasNext()) {
			sb.append(",");
			while (iter.hasNext()) {
				sb.append(iter.next().getName());
				columnCount++;
				if (iter.hasNext()) {
					sb.append(",");
				}
			}
		}
		
		sb.append(") VALUES (");
		for (int i=0; i<columnCount; i++) {
			if (i < (columnCount-1)) {
				sb.append("?,");
			} else {
				sb.append("?");
			}
		}
		sb.append(") ");
		super.appendWriteOptions(sb, colMutation.getTTL(), colMutation.getTimestamp());
		
		return sb.toString();
		
//		StringBuilder sb = new StringBuilder("UPDATE " + keyspace + "." + cf.getName()); 
//		super.appendWriteOptions(sb, colMutation.getTTL(), colMutation.getTimestamp());
//		sb.append(" SET ");
//		if (isCompositeKey) { 
//			String valueAlias = cfDef.getRegularColumnDefinitionList().get(0).getName();
//			sb.append(valueAlias).append(" = ? ");
//		} else {
//			sb.append(colMutation.columnName).append(" = ? ");
//		}
//		
//		return appendWhereClause(sb).toString();
	}
	
	
	private StringBuilder appendWhereClause(StringBuilder sb) { 
		
		Iterator<ColumnDefinition> iter = cfDef.getPartitionKeyColumnDefinitionList().iterator();
		
		sb.append(" WHERE ");
		while (iter.hasNext()) {
			sb.append(iter.next().getName()).append(" = ? ");
			if (iter.hasNext()) {
				sb.append("AND ");
			}
		}
		
		iter = cfDef.getClusteringKeyColumnDefinitionList().iterator();
		if (iter.hasNext()) {
			sb.append("AND ");
			while (iter.hasNext()) {
				sb.append(iter.next().getName()).append(" = ? ");
				if (iter.hasNext()) {
					sb.append("AND ");
				}
			}
		}
		
		return sb;
	}
	
	private Object[] getUpdateStatementBatchValues(CqlColumnMutationImpl<?,?> colMutation) {

		List<Object> objects = new ArrayList<Object>();

		
		objects.add(rowKey);
		
		Object columnName = colMutation.columnName;
		
		if (isCompositeColumn()) {
			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
			for (ComponentSerializer<?> component : compSerializer.getComponents()) {
				try {
					objects.add(component.getFieldValueDirectly(columnName));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		} else if (isCompositeKey) {
			objects.add(columnName);
		}

		objects.add(colMutation.columnValue);

		return objects.toArray();

//		List<Object> objects = new ArrayList<Object>();
//		objects.add(colMutation.columnValue);
//		
//		getBatchValuesForWhereClause(colMutation, objects);
//
//		return objects.toArray();
	}
	
	private void getBatchValuesForWhereClause(CqlColumnMutationImpl<?,?> colMutation, List<Object> list) {
		
		list.add(rowKey);
		
		Object columnName = colMutation.columnName;
		
		if (isCompositeColumn()) {
			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
			for (ComponentSerializer<?> component : compSerializer.getComponents()) {
				try {
					list.add(component.getFieldValueDirectly(columnName));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		} else if (isCompositeKey) {
			list.add(columnName);
		}
	}

	private String getDeleteColumnStatement(CqlColumnMutationImpl<?,?> colMutation) {

		StringBuilder sb = null; 
		if (isCompositeKey) {
			sb = new StringBuilder("DELETE FROM " + keyspace + "." + cf.getName());
		} else {
			sb = new StringBuilder("DELETE " + colMutation.columnName + " FROM " + keyspace + "." + cf.getName());
		}
		if (colMutation.getTimestamp() != null) {
			sb.append(" USING TIMESTAMP " + colMutation.getTimestamp());
		}
		return appendWhereClause(sb).toString();
	}

	private Object[] getDeleteStatementBatchValues(CqlColumnMutationImpl<?,?> colMutation) {
		
		List<Object> objects = new ArrayList<Object>();
		getBatchValuesForWhereClause(colMutation, objects);
		return objects.toArray();
	}

	private String getCounterColumnStatement(String keyspace, String cfName, boolean increment, CqlColumnMutationImpl<?,?> colMutation) {
		
		ColumnFamily<?,?> cf = cfContext.getColumnFamily();
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl)cf.getColumnFamilyDefinition();
		String valueAlias = cfDef.getRegularColumnDefinitionList().get(0).getName();

		StringBuilder sb = new StringBuilder();
		if (increment) {
			sb.append("UPDATE " + keyspace + "." + cfName); 
			super.appendWriteOptions(sb, colMutation.getTTL(), colMutation.getTimestamp());
			sb.append(" SET " + valueAlias + " = " + valueAlias + " + ? ");
		} else {
			sb.append("UPDATE " + keyspace + "." + cfName);
			super.appendWriteOptions(sb, colMutation.getTTL(), colMutation.getTimestamp());
			sb.append(" SET " + valueAlias + " = " + valueAlias + " - ? ");
		}
		
		String counterStatement = appendWhereClause(sb).toString();
		return counterStatement;
	}


	
	private Object[] getBatchValues(Object column, Object ... preObjectList) {
		
		List<Object> list = new ArrayList<Object>();
		for (Object obj : preObjectList) {
			list.add(obj);
		}
		
		if (isCompositeColumn()) {
			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
			for (ComponentSerializer<?> component : compSerializer.getComponents()) {
				try {
					list.add(component.getFieldValueDirectly(column));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		} else {
			list.add(column);
		}
		
		return list.toArray();
	}

	private boolean isCompositeColumn() {
		ColumnFamily<?,?> cf = cfContext.getColumnFamily();
		return cf.getColumnSerializer().getComparatorType() == ComparatorType.COMPOSITETYPE;
	}
	
	private static class KeyspaceColumnFamily { 
		private String keyspace;
		private String columnFamily; 
		private KeyspaceColumnFamily(String ks, String cf) {
			this.keyspace = ks;
			this.columnFamily = cf;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result	+ ((keyspace == null) ? 0 : keyspace.hashCode());
			result = prime * result	+ ((columnFamily == null) ? 0 : columnFamily.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			
			boolean equal = true;
			KeyspaceColumnFamily other = (KeyspaceColumnFamily) obj;
			equal = equal && (keyspace != null) ? (keyspace == other.keyspace): (other.keyspace == null);
			equal = equal && (columnFamily != null) ? (columnFamily == other.columnFamily): (other.columnFamily == null);
			return equal;
		}
	}
}
