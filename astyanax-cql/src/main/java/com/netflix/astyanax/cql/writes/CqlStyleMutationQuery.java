package com.netflix.astyanax.cql.writes;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.cql.util.ConsistencyLevelTransform;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CqlStyleMutationQuery {


	protected final String keyspace;
	protected final ColumnFamily<?,?> cf; 
	protected final Object rowKey;
	protected final List<CqlColumnMutationImpl> mutationList; 
	protected boolean deleteRow;
	protected final Long timestamp;
	protected final Integer ttl; 
	protected final ConsistencyLevel consistencyLevel;

	public CqlStyleMutationQuery(ChainedContext context, List<CqlColumnMutationImpl> mutationList, boolean deleteRow, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		
		context.rewindForRead(); 
		
		context.getNext(Cluster.class);
		this.keyspace = context.getNext(String.class);
		this.cf = context.getNext(ColumnFamily.class);
		this.rowKey = context.getNext(Object.class);
		this.mutationList = mutationList;
		this.deleteRow = deleteRow;
		this.timestamp = timestamp;
		this.ttl = ttl;
		this.consistencyLevel = consistencyLevel;
	}
	
	public String getDeleteEntireRowQuery() {
		return "DELETE FROM " + keyspace + "." + cf.getName() + " WHERE " + cf.getKeyAlias() + " = ?;";
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
