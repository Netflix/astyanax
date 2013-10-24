package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

@SuppressWarnings("deprecation")
public class CqlColumnListMutationImpl<K, C> extends AbstractColumnListMutationImpl<C> {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<K,C> cfContext;
	
	private final List<CqlColumnMutationImpl<?,?>> mutationList = new ArrayList<CqlColumnMutationImpl<?,?>>();
	private boolean deleteRow = false; 
	private com.netflix.astyanax.model.ConsistencyLevel consistencyLevel;
	
	private final CFMutationQueryGenerator queryGen; 
	
	public CqlColumnListMutationImpl(KeyspaceContext ksCtx, ColumnFamily<K,C> cf, K rowKey, ConsistencyLevel level, long timestamp) {
		
		super(timestamp);
		this.ksContext = ksCtx;
		this.cfContext = new ColumnFamilyMutationContext<K,C>(cf, rowKey);
		
		this.consistencyLevel = level;
		
		this.queryGen = new CFMutationQueryGenerator(ksContext, cfContext, mutationList, deleteRow, defaultTTL, defaultTimestamp, consistencyLevel);
	}
	
	@Override
	public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
		
		Preconditions.checkArgument(columnName != null, "Column Name must not be null");
		
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.putValue(value, valueSerializer, getActualTTL(ttl));
		
		mutationList.add(mutation);
		return this;
	}

	@Override
	public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {

		Integer theTTL = super.defaultTTL.get();
		if (ttl != null) {
			theTTL = ttl;
		}

		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.putEmptyColumn(theTTL);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
		
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.incrementCounterColumn(amount);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> deleteColumn(C columnName) {
		
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.deleteColumn();
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> delete() {
		deleteRow = true;
		return this;
	}
	
	@Override
    public ColumnListMutation<C> setDefaultTtl(Integer newTTL) {
		
		if (super.defaultTTL.get() == null) {
			defaultTTL.set(newTTL);
			return this;
		}
		
		if (!(defaultTTL.equals(newTTL))) {
			throw new RuntimeException("Default TTL has already been set, cannot reset");
		}
        return this;
    }
	
	public void mergeColumnListMutation(CqlColumnListMutationImpl<?, ?> colListMutation) {
		
		for (CqlColumnMutationImpl<?,?> colMutation : colListMutation.getMutationList()) {
			this.mutationList.add(colMutation);
		}
	}
	
	public List<CqlColumnMutationImpl<?,?>> getMutationList() {
		return mutationList;
	}
	
	public ColumnListMutation<C> putColumnWithGenericValue(C columnName, Object value, Integer ttl) {
		
		Preconditions.checkArgument(columnName != null, "Column Name must not be null");
		
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.putGenericValue(value, getActualTTL(ttl));
		
		mutationList.add(mutation);
		return this;
	}
	
	public BatchedStatements getBatch() {
		return queryGen.getQuery();
	}
	
	public List<Object> getBindValues() {
		return queryGen.getBindValues();
	}

	private Integer getActualTTL(Integer overrideTTL) {
		Integer theTTL = super.defaultTTL.get();
		if (overrideTTL != null) {
			theTTL = overrideTTL;
		}
		return theTTL;
	}
	
	public static class ColumnFamilyMutationContext<K,C> {
		
		private final ColumnFamily<K,C> columnFamily;
		private final K rowKey;
		private RetryPolicy retryPolicy;
		
		public ColumnFamilyMutationContext(ColumnFamily<K,C> cf, K rKey) {
			this.columnFamily = cf;
			this.rowKey = rKey;
			this.retryPolicy = null;
		}
		
		public ColumnFamilyMutationContext(ColumnFamily<K,C> cf, K rKey, RetryPolicy retry) {
			this.columnFamily = cf;
			this.rowKey = rKey;
			this.retryPolicy = retry;
		}

		public ColumnFamily<K, C> getColumnFamily() {
			return columnFamily;
		}

		public K getRowKey() {
			return rowKey;
		}

		public void setRetryPolicy(RetryPolicy retry) {
			this.retryPolicy = retry;
		}
		
		public RetryPolicy getRetryPolicy() {
			return retryPolicy;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("CF=").append(columnFamily.getName());
			sb.append(" RowKey: ").append(rowKey.toString());
			return sb.toString();
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(cfContext.toString());
		sb.append(" MutationList: ").append(mutationList.toString());
		return sb.toString();
	}
}