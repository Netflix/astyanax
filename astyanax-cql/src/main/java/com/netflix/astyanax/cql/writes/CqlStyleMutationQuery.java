package com.netflix.astyanax.cql.writes;

import java.util.List;

import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.util.ConsistencyLevelTransform;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CqlStyleMutationQuery {

	protected final KeyspaceContext ksContext;
	protected final ColumnFamilyMutationContext cfContext;
	protected final List<CqlColumnMutationImpl> mutationList; 
	protected boolean deleteRow;
	protected final Long timestamp;
	protected final Integer ttl; 
	protected final ConsistencyLevel consistencyLevel;

	public CqlStyleMutationQuery(KeyspaceContext ksCtx, ColumnFamilyMutationContext cfCtx, List<CqlColumnMutationImpl> mutationList, boolean deleteRow, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		
		this.mutationList = mutationList;
		this.deleteRow = deleteRow;
		this.timestamp = timestamp;
		this.ttl = ttl;
		this.consistencyLevel = consistencyLevel;
	}
	
	public String getDeleteEntireRowQuery() {
		ColumnFamily<?,?> cf = cfContext.getColumnFamily();
		return "DELETE FROM " + ksContext.getKeyspace() + "." + cf.getName() + " WHERE " + cf.getKeyAlias() + " = ?;";
	}

	public void appendWriteOptions(StringBuilder sb) {
		
		if (timestamp != null || ttl != null || consistencyLevel != null) {
			sb.append(" USING ");
		}
		
		if (ttl != null) {
			sb.append(" TTL " + ttl);
		}
		
		if (timestamp != null) {
			if (ttl != null) {
				sb.append(" AND");
			}
			sb.append(" TIMESTAMP " + timestamp);
		}
		
		if (consistencyLevel != null) {
			if (ttl != null || timestamp != null) {
				sb.append(" AND");
			}
			sb.append(" CONSISTENCY " + ConsistencyLevelTransform.getConsistencyLevel(consistencyLevel).name());
		}
	}
	
}
