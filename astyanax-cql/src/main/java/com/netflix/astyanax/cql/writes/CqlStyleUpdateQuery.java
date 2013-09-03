package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CqlStyleUpdateQuery extends CqlStyleMutationQuery {


	public CqlStyleUpdateQuery(ChainedContext context, List<CqlColumnMutationImpl> mutationList, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		super(context, mutationList, false, timestamp, ttl, consistencyLevel);
	}
	
	public BatchedStatements getQuery() {

		Preconditions.checkArgument(mutationList.size() > 0, "Empty mutation list");
		Preconditions.checkArgument(rowKey != null, "Row key must be provided");
		
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(keyspace + "." + cf.getName());
		
		appendWriteOptions(sb);
		
		sb.append(" SET ");

		List<Object> bindList = new ArrayList<Object>();
		
		Iterator<CqlColumnMutationImpl> iter = mutationList.iterator();
		while (iter.hasNext()) {
			CqlColumnMutationImpl m = iter.next();
			
			if (m.counterColumn) {
				long increment = ((Long)m.columnValue).longValue();
				if (increment < 0) {
					sb.append(" SET ").append(m.columnName).append(" = ").append(m.columnName).append(" - ?");
					m.columnValue = Math.abs(increment);
				} else {
					sb.append(" SET ").append(m.columnName).append(" = ").append(m.columnName).append(" + ?");
				} 
			} else {
				sb.append(m.columnName + " = ?");
			}
			
			bindList.add(m.columnValue);
			if (iter.hasNext()) {
				sb.append(", ");
			}
		}
		
		sb.append(" WHERE key = ?");

		bindList.add(rowKey);
		
		// sb1 + sb2
		String query = sb.toString();
		
		BatchedStatements batch = new BatchedStatements();
		batch.addBatch(query, bindList);
		return batch;
	}
}
