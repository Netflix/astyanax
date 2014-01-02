package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ConsistencyLevel;

@SuppressWarnings("deprecation")
public class CqlColumnListMutationImpl<K, C> extends AbstractColumnListMutationImpl<C> {

	public enum ColListMutationType {
		RowDelete, ColumnsUpdate, CounterColumnsUpdate;
	}
	
	private ColListMutationType type = ColListMutationType.ColumnsUpdate;
	
	private final KeyspaceContext ksContext;
	private final CFQueryContext<K,C> cfContext;
	
	private final CqlColumnFamilyDefinitionImpl cfDef;
	
	private final List<CqlColumnMutationImpl<K, C>> mutationList = new ArrayList<CqlColumnMutationImpl<K,C>>();
	private AtomicReference<Boolean> deleteRow = new AtomicReference<Boolean>(false); 
	
	public CqlColumnListMutationImpl(KeyspaceContext ksCtx, ColumnFamily<K,C> cf, K rowKey, ConsistencyLevel level, Long timestamp) {
		
		super(timestamp);
		this.ksContext = ksCtx;
		this.cfContext = new CFQueryContext<K,C>(cf, rowKey, null, level);
		this.cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
	}
	
	@Override
	public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
		
		checkColumnName(columnName);
		
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.putValue(value, valueSerializer, getActualTTL(ttl));
		if (this.getTimestamp() != null) {
			mutation.withTimestamp(this.getTimestamp());
		}
		
		mutationList.add(mutation);
		return this;
	}

	@Override
	public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {

		checkColumnName(columnName);
		
		Integer theTTL = super.defaultTTL.get();
		if (ttl != null) {
			theTTL = ttl;
		}

		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.putEmptyColumn(theTTL);
		if (this.getTimestamp() != null) {
			mutation.withTimestamp(this.getTimestamp());
		}
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
		
		checkColumnName(columnName);

		type = ColListMutationType.CounterColumnsUpdate;
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.incrementCounterColumn(amount);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> deleteColumn(C columnName) {
		
		checkColumnName(columnName);

		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.deleteColumn();
		if (this.getTimestamp() != null) {
			mutation.withTimestamp(this.getTimestamp());
		}
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> delete() {
		deleteRow.set(true);
		type = ColListMutationType.RowDelete;
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
			this.mutationList.add((CqlColumnMutationImpl<K, C>) colMutation);
		}
	}
	
	public List<CqlColumnMutationImpl<K,C>> getMutationList() {
		return mutationList;
	}
	
	public ColumnListMutation<C> putColumnWithGenericValue(C columnName, Object value, Integer ttl) {
		
		Preconditions.checkArgument(columnName != null, "Column Name must not be null");
		
		CqlColumnMutationImpl<K,C> mutation = new CqlColumnMutationImpl<K,C>(ksContext, cfContext, columnName);
		mutation.putGenericValue(value, getActualTTL(ttl));
		
		mutationList.add(mutation);
		return this;
	}
	
	private Integer getActualTTL(Integer overrideTTL) {
		Integer theTTL = super.defaultTTL.get();
		if (overrideTTL != null) {
			theTTL = overrideTTL;
		}
		return theTTL;
	}
	
	private void checkColumnName(C columnName) {
		Preconditions.checkArgument(columnName != null, "Column Name must not be null");
		if (columnName instanceof String) {
			Preconditions.checkArgument(!((String)columnName).isEmpty(), "Column Name must not be null");
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(cfContext.toString());
		sb.append(" MutationList: ").append(mutationList.toString());
		return sb.toString();
	}
	
	public CFMutationQueryGen getMutationQueryGen() {
		return cfDef.getMutationQueryGenerator();
	}

	public ColListMutationType getType() {
		return type;
	}

	public Object getRowKey() {
		return cfContext.getRowKey();
	}
}