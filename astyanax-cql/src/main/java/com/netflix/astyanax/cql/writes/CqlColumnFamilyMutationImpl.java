package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;

@SuppressWarnings("deprecation")
public class CqlColumnFamilyMutationImpl<K, C> extends AbstractColumnListMutationImpl<C> {

	private List<CqlColumnMutationImpl> mutationList = new ArrayList<CqlColumnMutationImpl>();
	private ColumnFamily<K, C> columnFamily; 
	private K rowKey;
	private String keyspace;
	private Cluster cluster; 
	private com.netflix.astyanax.model.ConsistencyLevel consistencyLevel;

	// Tracks the mutations on this ColumnFamily.
	private MutationState currentState = new InitialState();

	public CqlColumnFamilyMutationImpl(CqlMutationBatchImpl batchMutationImpl, ColumnFamily<K, C> cf, K rKey, long timestamp) {
		super(timestamp);
		this.columnFamily = cf;
		this.rowKey = rKey;
		this.keyspace = batchMutationImpl.getKeyspace();
		this.cluster = batchMutationImpl.getCluster();
		this.consistencyLevel = batchMutationImpl.getConsistencyLevel();
	}

	public String getName() {
		return columnFamily.getName();
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
		
		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(columnName.toString());
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

		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(columnName.toString());
		mutation.putEmptyColumn(ttl);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
		
		checkState(new UpdateColumnState());

		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(columnName.toString());
		mutation.incrementCounterColumn(amount);
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> deleteColumn(C columnName) {
		
		checkState(new UpdateColumnState());
		
		CqlColumnMutationImpl mutation = new CqlColumnMutationImpl(columnName.toString());
		mutation.deleteColumn();
		mutationList.add(mutation);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> delete() {
		checkState(new DeleteRowState());
		return this;
	}
	
	public BoundStatement getPreparedStatement() {
		return currentState.getStatement(this);
	}

	private interface MutationState { 
		public MutationState next(MutationState state);
		
		public BoundStatement getStatement(CqlColumnFamilyMutationImpl<?, ?> mutationImpl);
	}
	
	private class InitialState implements MutationState {

		@Override
		public MutationState next(MutationState state) {
			return state;
		}

		@Override
		public BoundStatement getStatement(CqlColumnFamilyMutationImpl<?, ?> mutationImpl) {
			throw new RuntimeException("Mutation list is empty");
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
		public BoundStatement getStatement(CqlColumnFamilyMutationImpl<?, ?> mutationImpl) {
			
			Preconditions.checkArgument(mutationList.size() > 0, "Empty mutation list");
			Preconditions.checkArgument(rowKey != null, "Row key must be provided");
			
			StringBuilder sb1 = new StringBuilder("UPDATE ");
			sb1.append( mutationImpl.keyspace + "." + mutationImpl.getName());
			
			appendWriteOptions(sb1);
			
			sb1.append(" SET ");

			List<Object> bindList = new ArrayList<Object>();
			
			Iterator<CqlColumnMutationImpl> iter = mutationImpl.mutationList.iterator();
			while (iter.hasNext()) {
				CqlColumnMutationImpl m = iter.next();
				
				if (m.counterColumn) {
					long increment = ((Long)m.columnValue).longValue();
					if (increment < 0) {
						sb1.append(" SET ").append(m.columnName).append(" = ").append(m.columnName).append(" - ?");
						m.columnValue = Math.abs(increment);
					} else {
						sb1.append(" SET ").append(m.columnName).append(" = ").append(m.columnName).append(" + ?");
					} 
				} else {
					sb1.append(m.columnName + " = ?");
				}
				
				bindList.add(m.columnValue);
				if (iter.hasNext()) {
					sb1.append(", ");
				}
			}
			
			sb1.append(" WHERE key = ?");

			bindList.add(mutationImpl.rowKey);
			
			// sb1 + sb2
			String query = sb1.toString();
			
			System.out.println("UPDATE query: " + query);
			
			Session session = mutationImpl.cluster.connect();
			PreparedStatement statement = session.prepare(query);

			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(bindList.toArray());
			
			return boundStatement;
		}
	}

	private void appendWriteOptions(StringBuilder sb) {
		CqlColumnMutationImpl.appendWriteOptions(sb, super.timestamp, super.defaultTTL, consistencyLevel);
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
		public BoundStatement getStatement(CqlColumnFamilyMutationImpl<?, ?> mutationImpl) {
			StringBuilder sb1 = new StringBuilder("INSERT INTO ");
			sb1.append( mutationImpl.keyspace + "." + mutationImpl.getName());
			
			sb1.append(" (key, ");

			StringBuilder sb2 = new StringBuilder(" VALUES (?, ");

			List<Object> bindList = new ArrayList<Object>();
			bindList.add(mutationImpl.rowKey);
			
			Iterator<CqlColumnMutationImpl> iter = mutationImpl.mutationList.iterator();
			while (iter.hasNext()) {
				CqlColumnMutationImpl m = iter.next();
				sb1.append(m.columnName);
				sb2.append("?");
				bindList.add(m.columnValue);
				if (iter.hasNext()) {
					sb1.append(", ");
					sb2.append(", ");
				}
			}
			
			sb1.append(")");
			sb2.append(")");
			
			appendWriteOptions(sb2);

			// sb1 + sb2
			String query = sb1.append(sb2.toString()).toString();
			
			System.out.println("Insert query: " + query);
			
			Session session = mutationImpl.cluster.connect();
			PreparedStatement statement = session.prepare(query);

			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(bindList.toArray());
			
			for (Object o : bindList) {
				System.out.print(String.valueOf(o) + " " );
			}
			System.out.println();
			
			return boundStatement;
		}
	}
	
	private class DeleteRowState implements MutationState {
		@Override
		public MutationState next(MutationState nextState) {
			throw new RuntimeException("Cannot use another mutation after delete() on this row");
		}

		@Override
		public BoundStatement getStatement(CqlColumnFamilyMutationImpl<?, ?> mutationImpl) {
			
			Preconditions.checkArgument(mutationImpl.mutationList.size() == 0, "Mutation list must be empty when deleting row");

			StringBuilder sb1 = new StringBuilder("DELETE FROM ");
			sb1.append( keyspace + "." + getName());
			
			appendWriteOptions(sb1);

			sb1.append(" WHERE key = ?");

			
			String query = sb1.toString();
			System.out.println("Delete query: " + query);
			
			Session session = cluster.connect();
			PreparedStatement statement = session.prepare(query);

			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(rowKey);
			return boundStatement;
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