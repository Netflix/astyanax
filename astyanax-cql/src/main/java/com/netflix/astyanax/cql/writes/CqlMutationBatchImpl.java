package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.ConsistencyLevelMapping;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColListMutationType;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

public class CqlMutationBatchImpl extends AbstractMutationBatchImpl {

	private final KeyspaceContext ksContext; 
	
	// Control to turn use of prepared statement caching ON/OFF
	private boolean useCaching = false;
	
	public CqlMutationBatchImpl(KeyspaceContext ksCtx, Clock clock, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
		super(clock, consistencyLevel, retry);
		this.ksContext = ksCtx;
	}

	@Override
	public <K, C> ColumnListMutation<C> createColumnListMutation(String keyspace, ColumnFamily<K, C> cf, K rowKey) {
		return new CqlColumnListMutationImpl<K, C>(ksContext, cf, rowKey, getConsistencyLevel(), timestamp);
	}

	@Override
	public void mergeColumnListMutation(ColumnListMutation<?> from, ColumnListMutation<?> to) {
	
		CqlColumnListMutationImpl<?, ?> fromCqlListMutation = (CqlColumnListMutationImpl<?, ?>) from;
		CqlColumnListMutationImpl<?, ?> toCqlListMutation = (CqlColumnListMutationImpl<?, ?>) to;
		
		toCqlListMutation.mergeColumnListMutation(fromCqlListMutation);
	}

	@Override
	public OperationResult<Void> execute() throws ConnectionException {
		
		return new CqlAbstractExecutionImpl<Void>(ksContext, getRetryPolicy()) {

			@Override
			public CassandraOperationType getOperationType() {
				return CassandraOperationType.BATCH_MUTATE;
			}

			@Override
			public Statement getQuery() {
				return getCachedPreparedStatement();
			}

			@Override
			public Void parseResultSet(ResultSet resultSet) {
				return null; // do nothing for mutations
			}
		}.execute();
	}

	@Override
	public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
		
		return new CqlAbstractExecutionImpl<Void>(ksContext, getRetryPolicy()) {

			@Override
			public CassandraOperationType getOperationType() {
				return CassandraOperationType.BATCH_MUTATE;
			}

			@Override
			public Statement getQuery() {
				return getCachedPreparedStatement();
			}

			@Override
			public Void parseResultSet(ResultSet resultSet) {
				return null; // do nothing for mutations
			}
		}.executeAsync();
	}

	private List<CqlColumnListMutationImpl<?, ?>> getColumnMutations() {
		
		List<CqlColumnListMutationImpl<?,?>> colListMutation = new ArrayList<CqlColumnListMutationImpl<?,?>>();
		
		for (Entry<ByteBuffer, Map<String, ColumnListMutation<?>>> entry : super.getMutationMap().entrySet()) {
			for (ColumnListMutation<?> colMutation : entry.getValue().values()) {
				colListMutation.add((CqlColumnListMutationImpl<?, ?>) colMutation);
			}
		}
		return colListMutation;
	}

	private BatchStatement getCachedPreparedStatement() {
		
		final List<CqlColumnListMutationImpl<?, ?>> colListMutations = getColumnMutations();

		if (colListMutations == null || colListMutations.size() == 0) {
			return new BatchStatement(Type.UNLOGGED);
		}
		
		ColListMutationType mutationType = colListMutations.get(0).getType();

		BatchStatement batch = new BatchStatement(Type.UNLOGGED);
		if (mutationType == ColListMutationType.CounterColumnsUpdate) {
			batch = new BatchStatement(Type.COUNTER);
		} else if (useAtomicBatch()) {
			batch = new BatchStatement(Type.LOGGED);
		}
		
		for (CqlColumnListMutationImpl<?, ?> colListMutation : colListMutations) {
			
			CFMutationQueryGen queryGen = colListMutation.getMutationQueryGen();
			queryGen.addColumnListMutationToBatch(batch, colListMutation, useCaching);
		}
		
		batch.setConsistencyLevel(ConsistencyLevelMapping.getCL(this.getConsistencyLevel()));
		
		return batch;
	}

	@Override
	public MutationBatch withCaching(boolean condition) {
		useCaching = condition;
		return this;
	}
}
