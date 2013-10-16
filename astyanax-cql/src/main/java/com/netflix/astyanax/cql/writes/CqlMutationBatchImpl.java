package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

public class CqlMutationBatchImpl extends AbstractMutationBatchImpl {

	private final KeyspaceContext ksContext; 
	
	public CqlMutationBatchImpl(KeyspaceContext ksCtx, Clock clock, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
		super(clock, consistencyLevel, retry);
		this.ksContext = ksCtx;
	}

	@Override
	public <K, C> ColumnListMutation<C> createColumnMutation(String keyspace, ColumnFamily<K, C> cf, K rowKey) {
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
		
		BoundStatement statement = getCachedPreparedStatement();
		ResultSet rs = ksContext.getSession().execute(statement);
		return new CqlOperationResultImpl<Void>(rs, null);
	}

	@Override
	public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {

		if (super.rowLookup.values().size() > 1) {
			throw new NotImplementedException();
		}
		
		BoundStatement statement = getCachedPreparedStatement();
		ResultSetFuture rsFuture = ksContext.getSession().executeAsync(statement);
		return new AsyncOperationResult<Void>(rsFuture) {
			@Override
			public OperationResult<Void> getOperationResult(ResultSet rs) {
				return new CqlOperationResultImpl<Void>(rs, null);
			}
		};
	}
	
//	private BoundStatement getTotalStatement() {
//		BatchedStatements statements = new BatchedStatements();
//		
//		for (CqlColumnFamilyMutationImpl<?, ?> cfMutation : rowLookup.values()) {
//			statements.addBatch(cfMutation.getBatch());
//		}
//		
//		return statements.getBoundStatement(session, useAtomicBatch);
//	}

	private List<CqlColumnListMutationImpl<?, ?>> getColumnMutations() {
		
		List<CqlColumnListMutationImpl<?,?>> colListMutation = new ArrayList<CqlColumnListMutationImpl<?,?>>();
		
		for (Entry<ByteBuffer, Map<String, ColumnListMutation<?>>> entry : super.getMutationMap().entrySet()) {
			colListMutation.addAll((Collection<? extends CqlColumnListMutationImpl<?, ?>>) entry.getValue().values());
		}
		return colListMutation;
	}

	private BoundStatement getCachedPreparedStatement() {
		
		//Integer id = CqlColumnListMutationImpl.class.getName().hashCode();
		final List<CqlColumnListMutationImpl<?, ?>> colListMutations = getColumnMutations();
		final List<Object> bindValues = new ArrayList<Object>();
		
//		PreparedStatement pStmt = StatementCache.getInstance().getStatement(id, new Callable<PreparedStatement>() {
//
//			@Override
//			public PreparedStatement call() throws Exception {
//				BatchedStatements statements = new BatchedStatements();
//				for (CqlColumnListMutationImpl<?, ?> cfMutation : colListMutations) {
//					statements.addBatch(cfMutation.getBatch());
//				}
//				bindValues.addAll(statements.getBatchValues());
//				
//				PreparedStatement preparedStmt = ksContext.getSession().prepare(statements.getBatchQuery(false));
//				return preparedStmt;
//			}
//		});
		
		BatchedStatements statements = new BatchedStatements();
		for (CqlColumnListMutationImpl<?, ?> cfMutation : colListMutations) {
			statements.addBatch(cfMutation.getBatch());
		}
		bindValues.addAll(statements.getBatchValues());
		
		PreparedStatement pStmt = ksContext.getSession().prepare(statements.getBatchQuery(false));

		if (bindValues.size() == 0) {
			for (CqlColumnListMutationImpl<?, ?> cfMutation : colListMutations) {
				bindValues.addAll(cfMutation.getBindValues());
			}
		}
		
		BoundStatement bStmt = pStmt.bind(bindValues.toArray());
		return bStmt;
	}
}
