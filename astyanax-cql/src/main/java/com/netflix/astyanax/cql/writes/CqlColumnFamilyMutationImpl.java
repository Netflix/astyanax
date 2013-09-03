package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ConsistencyLevel;

@SuppressWarnings("deprecation")
public class CqlColumnFamilyMutationImpl<K, C> extends AbstractColumnListMutationImpl<C> {

	private ChainedContext context; 
	
	private List<CqlColumnMutationImpl> mutationList = new ArrayList<CqlColumnMutationImpl>();
	private boolean deleteRow = false; 
	
//	private ColumnFamily<K, C> columnFamily; 
//	private K rowKey;
//	private String keyspace;
//	private Cluster cluster; 
	private com.netflix.astyanax.model.ConsistencyLevel consistencyLevel;

	// Tracks the mutations on this ColumnFamily.
	private MutationState currentState = new InitialState();

	public CqlColumnFamilyMutationImpl(ChainedContext context, ConsistencyLevel level, long timestamp) {
		super(timestamp);
		
		this.context = context;
		context.rewindForRead(); 
		
//		cluster = context.getNext(Cluster.class);
//		keyspace = context.getNext(String.class);
//		columnFamily = context.getNext(ColumnFamily.class);
//		rowKey = (K) context.getNext(Object.class);

		this.consistencyLevel = level;
		
		if (CqlFamilyFactory.OldStyleThriftMode()) {
			currentState = new OldStyleThriftState();
		}
	}

//	public String getColumnFamilyName() {
//		return columnFamily.getName();
//	}
	
	
	@Override
	public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
		
		System.out.println("Columnname : " + columnName + " " + valueSerializer.getComparatorType() + " " + valueSerializer.getClass().getName());
		Preconditions.checkArgument(columnName != null, "Column Name must not be null");
		
		if (currentState instanceof CqlColumnFamilyMutationImpl.InitialState || 
				currentState instanceof CqlColumnFamilyMutationImpl.NewRowState) {
			checkState(new NewRowState());
		} else {
			checkState(new UpdateColumnState());
		}
		checkAndSetTTL(ttl);
		
		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(context.clone().add(columnName));
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

		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(context.clone().add(columnName));
		mutation.putEmptyColumn(ttl);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
		
		checkState(new UpdateColumnState());

		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(context.clone().add(columnName));
		mutation.incrementCounterColumn(amount);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> deleteColumn(C columnName) {
		
		checkState(new UpdateColumnState());
		
		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(context.clone().add(columnName));
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

	private interface MutationState { 
		public MutationState next(MutationState state);
		public BatchedStatements getStatement();
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
			return new OldStyleCFMutationQuery(context.clone(), mutationList, deleteRow, timestamp, defaultTTL, consistencyLevel).getQuery();
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
			return new CqlStyleUpdateQuery(context.clone(), mutationList, timestamp, defaultTTL, consistencyLevel).getQuery();
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
			return new CqlStyleInsertQuery(context.clone(), mutationList, timestamp, defaultTTL, consistencyLevel).getQuery();
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
			return new CqlStyleDeleteRowQuery(context.clone(), mutationList, timestamp, defaultTTL, consistencyLevel).getQuery();
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
}