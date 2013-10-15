package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ConsistencyLevel;

@SuppressWarnings("deprecation")
public class CqlColumnFamilyMutationImpl<K, C> extends AbstractColumnListMutationImpl<C> {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<K,C> cfContext;
	
	private List<CqlColumnMutationImpl> mutationList = new ArrayList<CqlColumnMutationImpl>();
	private boolean deleteRow = false; 
	
	private com.netflix.astyanax.model.ConsistencyLevel consistencyLevel;
	
	// Tracks the mutations on this ColumnFamily.
	private MutationState currentState = new InitialState();

	public CqlColumnFamilyMutationImpl(KeyspaceContext ksCtx, ColumnFamily<K,C> cf, K rowKey, ConsistencyLevel level, long timestamp) {
		
		super(timestamp);
		this.ksContext = ksCtx;
		this.cfContext = new ColumnFamilyMutationContext<K,C>(cf, rowKey);
		
		this.consistencyLevel = level;
		
		if (CqlFamilyFactory.OldStyleThriftMode()) {
			currentState = new OldStyleThriftState();
		}
	}

	
	@Override
	public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
		
		Preconditions.checkArgument(columnName != null, "Column Name must not be null");
		
		if (currentState instanceof CqlColumnFamilyMutationImpl.InitialState || 
				currentState instanceof CqlColumnFamilyMutationImpl.NewRowState) {
			checkState(new NewRowState());
		} else {
			checkState(new UpdateColumnState());
		}
		checkAndSetTTL(ttl);
		
		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(ksContext, cfContext, columnName);
		mutation.putValue(value, valueSerializer, ttl);
		
		mutationList.add(mutation);
		return this;
	}

	@Override
	public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {

		checkAndSetTTL(ttl);

		if (currentState instanceof CqlColumnFamilyMutationImpl.InitialState || 
				currentState instanceof CqlColumnFamilyMutationImpl.NewRowState) {
			checkState(new NewRowState());
		} else {
			checkState(new UpdateColumnState());
		}

		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(ksContext, cfContext, columnName);
		mutation.putEmptyColumn(ttl);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
		
		checkState(new UpdateColumnState());

		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(ksContext, cfContext, columnName);
		mutation.incrementCounterColumn(amount);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> deleteColumn(C columnName) {
		
		checkState(new UpdateColumnState());
		
		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(ksContext, cfContext, columnName);
		mutation.deleteColumn();
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> delete() {
		deleteRow = true;
		checkState(new DeleteRowState());
		return this;
	}
	
	public BatchedStatements getBatch() {
		return currentState.getStatement();
	}
	
	public List<Object> getBindValues() {
		return currentState.getBindValues();
	}

	private interface MutationState { 
		public MutationState next(MutationState state);
		public BatchedStatements getStatement();
		public List<Object> getBindValues();
	}
	
	private class InitialState implements MutationState {

		@Override
		public MutationState next(MutationState state) {
			return state;
		}

		@Override
		public BatchedStatements getStatement() {
			throw new RuntimeException("Mutation list is empty");
		}

		@Override
		public List<Object> getBindValues() {
			throw new RuntimeException("Mutation list is empty");
		}
	}
	
	private class OldStyleThriftState implements MutationState {

		@Override
		public MutationState next(MutationState state) {
			if (state instanceof CqlColumnFamilyMutationImpl.DeleteRowState) {
				return state;
			}
			return this;  // once in this state - stay in same state
		}

		@Override
		public BatchedStatements getStatement() {
			return new OldStyleCFMutationQuery(ksContext, cfContext, mutationList, deleteRow, timestamp, defaultTTL, consistencyLevel).getQuery();
		}
		
		@Override
		public List<Object> getBindValues() {
			return new OldStyleCFMutationQuery(ksContext, cfContext, mutationList, deleteRow, timestamp, defaultTTL, consistencyLevel).getBindValues();
		}

	}

	private class UpdateColumnState implements MutationState {

		@Override
		public MutationState next(MutationState nextState) {
			if (! (nextState instanceof CqlColumnFamilyMutationImpl.UpdateColumnState)) {
				throw new RuntimeException("Must only call PutColumn for this ColumnFamily mutation");
			}
			return nextState;
		}

		@Override
		public BatchedStatements getStatement() {
			return null; // TODO
			//return new CqlStyleUpdateQuery(context.clone(), mutationList, timestamp, defaultTTL, consistencyLevel).getQuery();
		}

		@Override
		public List<Object> getBindValues() {
			throw new RuntimeException("Not implemented");
		}
	}

	private class NewRowState implements MutationState {

		@Override
		public MutationState next(MutationState nextState) {
			if (nextState instanceof CqlColumnFamilyMutationImpl.UpdateColumnState) {
				return nextState;
			}
			if (nextState instanceof CqlColumnFamilyMutationImpl.NewRowState) {
				return nextState;
			}
			
			throw new RuntimeException("Must use only putColumn() or deleteColumn() for this BatchMutation");
		}

		@Override
		public BatchedStatements getStatement() {
			return null; // TODO
			//			return new CqlStyleInsertQuery(context.clone(), mutationList, timestamp, defaultTTL, consistencyLevel).getQuery();
		}

		@Override
		public List<Object> getBindValues() {
			throw new RuntimeException("Not implemented");
		}
	}
	
	private class DeleteRowState implements MutationState {
		@Override
		public MutationState next(MutationState nextState) {
			throw new RuntimeException("Cannot use another mutation after delete() on this row");
		}

		@Override
		public BatchedStatements getStatement() {
			
			Preconditions.checkArgument(mutationList == null || mutationList.size() == 0, "Mutation list must be empty when deleting row");
			return null; // TODO
			//			return new CqlStyleDeleteRowQuery(context.clone(), mutationList, timestamp, defaultTTL, consistencyLevel).getQuery();
		}

		@Override
		public List<Object> getBindValues() {
			throw new RuntimeException("Not implemented");
		}
	}
	
	private void checkState(MutationState nextState) {
		currentState = currentState.next(nextState);
	}
	
	private void checkAndSetTTL(Integer newTTL) {
		if (super.defaultTTL == null) {
			defaultTTL = newTTL;
			return;
		}
		
		if (!(defaultTTL.equals(newTTL))) {
			throw new RuntimeException("Default TTL has already been set, cannot reset");
		}
	}
	
	@Override
    public ColumnListMutation<C> setDefaultTtl(Integer ttl) {
		checkAndSetTTL(ttl);
        return this;
    }
	
	public static class ColumnFamilyMutationContext<K,C> {
		
		private final ColumnFamily<K,C> columnFamily;
		private final K rowKey;
		
		public ColumnFamilyMutationContext(ColumnFamily<K,C> cf, K rKey) {
			this.columnFamily = cf;
			this.rowKey = rKey;
		}
		
		public ColumnFamily<K, C> getColumnFamily() {
			return columnFamily;
		}

		public K getRowKey() {
			return rowKey;
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