package com.netflix.astyanax.cql.writes;

import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.cql.util.ChainedContext2;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CqlStyleDeleteRowQuery extends CqlStyleMutationQuery {

	public CqlStyleDeleteRowQuery(ChainedContext2 context, List<CqlColumnMutationImpl> mutationList, Long timestamp, Integer ttl, ConsistencyLevel consistencyLevel) {
		//super(context, mutationList, true, timestamp, ttl, consistencyLevel);
		// TODO
		super(null, null, mutationList, true, timestamp, ttl, consistencyLevel);
		throw new RuntimeException("Fix this");
	}

	public BatchedStatements getQuery() {
		Preconditions.checkArgument(mutationList == null || mutationList.size() == 0, "Mutation list must be empty when deleting row");
		BatchedStatements batch = new BatchedStatements();
		//batch.addBatch(super.getDeleteEntireRowQuery(), null /** TODO fix this */);
		return batch;
	}
}
