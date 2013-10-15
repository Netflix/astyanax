package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.writes.CqlColumnFamilyMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

public class OldStyleCFMutationQuery extends CqlStyleMutationQuery {

	private Map<KeyspaceColumnFamily, String> updateStatementCache  = new HashMap<KeyspaceColumnFamily, String>();
	private Map<KeyspaceColumnFamily, String> deleteStatementCache  = new HashMap<KeyspaceColumnFamily, String>();
	private Map<KeyspaceColumnFamily, String> counterIncrStatementCache = new HashMap<KeyspaceColumnFamily, String>();
	private Map<KeyspaceColumnFamily, String> counterDecrStatementCache = new HashMap<KeyspaceColumnFamily, String>();

	public OldStyleCFMutationQuery(KeyspaceContext ksContext, ColumnFamilyMutationContext cfContext, 
								   List<CqlColumnMutationImpl> mutationList, boolean deleteRow, 
								   Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		super(ksContext, cfContext, mutationList, deleteRow, timestamp, ttl, consistencyLevel);
	}

	public List<Object> getBindValues() {
		
		List<Object> values = new ArrayList<Object>();
		
		if (deleteRow) {
			Preconditions.checkArgument(mutationList.size() == 0, "Cannot delete row with pending column mutations");
			values.add(super.cfContext.getRowKey());
			return values;
		}

		for (CqlColumnMutationImpl colMutation : mutationList) {

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

		return values;
	}
	
	public BatchedStatements getQuery() {
		
		BatchedStatements statements = new BatchedStatements();
		
		if (deleteRow) {
			Preconditions.checkArgument(mutationList.size() == 0, "Cannot delete row with pending column mutations");
			statements.addBatch(super.getDeleteEntireRowQuery(), super.cfContext.getRowKey());
			return statements;
		}

		for (CqlColumnMutationImpl colMutation : mutationList) {

			String keyspace = ksContext.getKeyspace();
			ColumnFamily<?,?> cf = cfContext.getColumnFamily();
			Object rowKey = cfContext.getRowKey();
			
			if (colMutation.deleteColumn) {

				statements.addBatchQuery(getDeleteColumnStatement(keyspace, cf.getName()));
				statements.addBatchValues(getBatchValues(colMutation.columnName, rowKey));

			} else if (colMutation.counterColumn) {

				Integer delta = (Integer) colMutation.columnValue;
				boolean increment = true;
				if (delta < 0) {
					increment = false;
					delta = Math.abs(delta);
				}
				statements.addBatchQuery(getCounterColumnStatement(keyspace, cf.getName(), increment));
				statements.addBatchValues(getBatchValues(colMutation.columnName, delta, rowKey));
			} else {
				statements.addBatchQuery(getUpdateColumnStatement(keyspace, cf.getName()));
				statements.addBatchValues(getBatchValues(colMutation.columnName, colMutation.columnValue, rowKey));
			}
		}

		return statements;
	}
	
	
	private String getUpdateColumnStatement(String keyspace, String cfName) {

		KeyspaceColumnFamily kfCF = new KeyspaceColumnFamily(keyspace, cfName);
		
		String updateStatement = updateStatementCache.get(kfCF);
		if (updateStatement == null) {
			StringBuilder sb = new StringBuilder("UPDATE " + keyspace + "." + cfName + " SET value = ?");
			updateStatement = appendWhereClause(sb).toString();
			updateStatementCache.put(kfCF, updateStatement);
		}
		
		return updateStatement;
	}

	private String getDeleteColumnStatement(String keyspace, String cfName) {

		KeyspaceColumnFamily kfCF = new KeyspaceColumnFamily(keyspace, cfName);
		
		String deleteStatement = deleteStatementCache.get(kfCF);
		if (deleteStatement == null) {
			StringBuilder sb = new StringBuilder("DELETE FROM " + keyspace + "." + cfName);
			deleteStatement = appendWhereClause(sb).toString();
			deleteStatementCache.put(kfCF, deleteStatement);
		}
		
		return deleteStatement;
	}

	private String getCounterColumnStatement(String keyspace, String cfName, boolean increment) {
		
		KeyspaceColumnFamily kfCF = new KeyspaceColumnFamily(keyspace, cfName);

		Map<KeyspaceColumnFamily, String> cache = increment ? counterIncrStatementCache : counterDecrStatementCache;
		
		String counterStatement = cache.get(kfCF);
		if (counterStatement == null) {
			StringBuilder sb = new StringBuilder();
			if (increment) {
				sb.append("UPDATE " + keyspace + "." + cfName + " SET value = value + ? ");
			} else {
				sb.append("UPDATE " + keyspace + "." + cfName + " SET value = value - ? ");
			}
			counterStatement = appendWhereClause(sb).toString();
			cache.put(kfCF, counterStatement);
		}
		
		return counterStatement;
	}

	private StringBuilder appendWhereClause(StringBuilder sb) { 
		
		ColumnFamily<?,?> cf = cfContext.getColumnFamily();

		if (isCompositeColumn()) {
			
			AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
			
			int numComponents = compSerializer.getComponents().size(); 
			sb.append(" WHERE key = ?");
			for (int i=1; i<=numComponents; i++) {
				sb.append(" and column").append(i).append(" = ?");
			}
			sb.append(";\n");
			
		} else {
			
			sb.append(" WHERE key = ? and column1 = ?; \n");
		}
		return sb;
	}
	
	private Object[] getBatchValues(Object column, Object ... preObjectList) {
		
		List<Object> list = new ArrayList<Object>();
		for (Object obj : preObjectList) {
			list.add(obj);
		}
		
		if (isCompositeColumn()) {
			ColumnFamily<?,?> cf = cfContext.getColumnFamily();
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
