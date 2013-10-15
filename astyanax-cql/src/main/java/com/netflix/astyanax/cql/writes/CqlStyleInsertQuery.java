package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.cql.util.ChainedContext2;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CqlStyleInsertQuery extends CqlStyleMutationQuery {
	
	public CqlStyleInsertQuery(ChainedContext2 context, List<CqlColumnMutationImpl> mutationList, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		super(null, null, mutationList, false, timestamp, ttl, consistencyLevel);
		throw new RuntimeException("Fix this");  // TODO fix this
	}
	
	public BatchedStatements getQuery() {
		
		Preconditions.checkArgument(mutationList.size() > 0, "Empty mutation list");
//		Preconditions.checkArgument(rowKey != null, "Row key must be provided");
//
		StringBuilder sb1 = new StringBuilder("INSERT INTO ");
//		sb1.append(keyspace + "." + cf.getName());
//	
//		sb1.append(" (" + cf.getKeyAlias() + ",");
//
		StringBuilder sb2 = new StringBuilder(" VALUES (?, ");
//
		List<Object> bindList = new ArrayList<Object>();
//		bindList.add(rowKey);
//	
//		Iterator<CqlColumnMutationImpl> iter = mutationList.iterator();
//		
//		while (iter.hasNext()) {
//			CqlColumnMutationImpl m = iter.next();
//			sb1.append(m.columnName);
//			sb2.append("?");
//			bindList.add(m.columnValue);
//			if (iter.hasNext()) {
//				sb1.append(", ");
//				sb2.append(", ");
//			}
//		}
	
		sb1.append(")");
		sb2.append(")");
	
		appendWriteOptions(sb2);

		// sb1 + sb2
		String query = sb1.append(sb2.toString()).toString();
	
		BatchedStatements batch = new BatchedStatements();
		batch.addBatch(query, bindList);
		return batch;
	}
}
