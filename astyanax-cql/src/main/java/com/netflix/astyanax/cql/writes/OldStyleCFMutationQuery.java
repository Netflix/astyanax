package com.netflix.astyanax.cql.writes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ConsistencyLevel;

public class OldStyleCFMutationQuery extends CqlStyleMutationQuery {

	private Map<KeyspaceColumnFamily, String> updateStatementCache  = new HashMap<KeyspaceColumnFamily, String>();
	private Map<KeyspaceColumnFamily, String> deleteStatementCache  = new HashMap<KeyspaceColumnFamily, String>();
	private Map<KeyspaceColumnFamily, String> counterIncrStatementCache = new HashMap<KeyspaceColumnFamily, String>();
	private Map<KeyspaceColumnFamily, String> counterDecrStatementCache = new HashMap<KeyspaceColumnFamily, String>();

	public OldStyleCFMutationQuery(ChainedContext context, List<CqlColumnMutationImpl> mutationList, boolean deleteRow, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		super(context, mutationList, deleteRow, timestamp, ttl, consistencyLevel);
	}
	
	public BatchedStatements getQuery() {

		BatchedStatements statements = new BatchedStatements();
		
		if (deleteRow) {
			Preconditions.checkArgument(mutationList.size() == 0, "Cannot delete row with pending column mutations");
			statements.addBatch(super.getDeleteEntireRowQuery(), super.rowKey);
			return statements;
		}

		for (CqlColumnMutationImpl colMutation : mutationList) {

			if (colMutation.deleteColumn) {

				statements.addBatchQuery(getDeleteColumnStatement(keyspace, cf.getName()));
				statements.addBatchValues(rowKey, colMutation.columnName);

			} else if (colMutation.counterColumn) {

				Integer delta = (Integer) colMutation.columnValue;
				boolean increment = true;
				if (delta < 0) {
					increment = false;
					delta = Math.abs(delta);
				}
				statements.addBatchQuery(getCounterColumnStatement(keyspace, cf.getName(), increment));
				statements.addBatchValues(delta, rowKey, colMutation.columnName);
			} else {
				statements.addBatchQuery(getUpdateColumnStatement(keyspace, cf.getName()));
				statements.addBatchValues(colMutation.columnValue, rowKey, colMutation.columnName);
			}
		}

		return statements;
	}
	
	
	private String getUpdateColumnStatement(String keyspace, String cfName) {

		KeyspaceColumnFamily kfCF = new KeyspaceColumnFamily(keyspace, cfName);
		
		String updateStatement = updateStatementCache.get(kfCF);
		if (updateStatement == null) {
			updateStatement = "UPDATE " + keyspace + "." + cfName + " SET value = ? WHERE key = ? and column1 = ? ; \n";
			updateStatementCache.put(kfCF, updateStatement);
		}
		
		return updateStatement;
	}

	private String getDeleteColumnStatement(String keyspace, String cfName) {

		KeyspaceColumnFamily kfCF = new KeyspaceColumnFamily(keyspace, cfName);
		
		String updateStatement = deleteStatementCache.get(kfCF);
		if (updateStatement == null) {
			updateStatement = "DELETE FROM " + keyspace + "." + cfName + " WHERE key = ? and column1 = ? ; \n";
			deleteStatementCache.put(kfCF, updateStatement);
		}
		
		return updateStatement;
	}

	private String getCounterColumnStatement(String keyspace, String cfName, boolean increment) {
		
		KeyspaceColumnFamily kfCF = new KeyspaceColumnFamily(keyspace, cfName);

		Map<KeyspaceColumnFamily, String> cache = increment ? counterIncrStatementCache : counterDecrStatementCache;
		
		String counterStatement = cache.get(kfCF);
		if (counterStatement == null) {
			if (increment) {
				counterStatement = "UPDATE " + keyspace + "." + cfName + " SET value = value + ? WHERE key = ? and column1 = ?; \n";
			} else {
				counterStatement = "UPDATE " + keyspace + "." + cfName + " SET value = value - ? WHERE key = ? and column1 = ?; \n";
			}
			cache.put(kfCF, counterStatement);
		}
		
		return counterStatement;
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
