package com.netflix.astyanax.cql.writes;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CqlStyleMutationQuery {

	protected final KeyspaceContext ksContext;
	protected final CFQueryContext<?,?> cfContext;
	protected final List<CqlColumnMutationImpl<?,?>> mutationList;
	
	protected AtomicReference<Boolean> deleteRow;
	
	protected final AtomicReference<Long> defaultTimestamp;
	protected final AtomicReference<Integer> defaultTTL; 
	protected final ConsistencyLevel consistencyLevel;

	private static final String USING = " USING ";
	private static final String TTL = " TTL ";
	private static final String AND = " AND";
	private static final String TIMESTAMP = " TIMESTAMP ";
	
	public CqlStyleMutationQuery(KeyspaceContext ksCtx, CFQueryContext<?,?> cfCtx, 
								 List<CqlColumnMutationImpl<?,?>> mutationList, AtomicReference<Boolean> deleteRow, 
								 AtomicReference<Integer> ttl, AtomicReference<Long> timestamp, ConsistencyLevel consistencyLevel) {
		
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		
		this.mutationList = mutationList;
		this.deleteRow = deleteRow;
		this.defaultTTL = ttl;
		this.defaultTimestamp = timestamp;
		this.consistencyLevel = consistencyLevel;
		
		if (this.consistencyLevel != null) {
			cfContext.setConsistencyLevel(consistencyLevel);
		}
	}
	
	public String getDeleteEntireRowQuery() {
		ColumnFamily<?,?> cf = cfContext.getColumnFamily();
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl)cf.getColumnFamilyDefinition();
		return "DELETE FROM " + ksContext.getKeyspace() + "." + cf.getName() + 
				" WHERE " + cfDef.getPartitionKeyColumnDefinition().getName() + " = ?;";
	}

	public void appendWriteOptions(StringBuilder sb, Integer overrideTTL, Long overrideTimestamp) {
		
		Integer ttl = overrideTTL != null ? overrideTTL : defaultTTL.get();
		Long timestamp = overrideTimestamp != null ? overrideTimestamp : defaultTimestamp.get();
				
		if (ttl != null || timestamp != null) {
			sb.append(USING);
		}
		
		if (ttl != null) {
			sb.append(TTL + ttl);
		}
		
		if (timestamp != null) {
			if (ttl != null) {
				sb.append(AND);
			}
			sb.append(TIMESTAMP + timestamp);
		}
	}
	
}
