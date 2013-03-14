package com.netflix.astyanax.index;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AbstractColumnListMutation;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.thrift.ThriftColumnFamilyMutationImpl;

public class HCMutationBatchImpl implements MutationBatch {

	private IndexCoordination indexcoorindator;
	private MutationBatch mutator;
	
	public HCMutationBatchImpl(MutationBatch mutator) {
		this.indexcoorindator = IndexCoordinationFactory.getIndexContext();
		this.mutator = mutator;
	}
	public HCMutationBatchImpl(MutationBatch mutator, IndexCoordination coordinator) {		
		this.indexcoorindator = coordinator;
		this.mutator = mutator;
	}
			
	public IndexCoordination getIndexcoorindator() {
		return indexcoorindator;
	}
	public void setIndexcoorindator(IndexCoordination indexcoorindator) {
		this.indexcoorindator = indexcoorindator;
	}
	
	@Override
	public <K, C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
		//wrap and return wrapping
		//this might throw if its the super column implementation :(
		ThriftColumnFamilyMutationImpl<C> impl =  (ThriftColumnFamilyMutationImpl<C>)mutator.withRow(columnFamily, rowKey);
										
		ThriftMutatorExt<C,K> cfMutatorWrapper = new ThriftMutatorExt<C,K>(mutator,impl,indexcoorindator,columnFamily, rowKey); 
				
		return cfMutatorWrapper;
	}

	@Override
	public <K> void deleteRow(Iterable<? extends ColumnFamily<K, ?>> columnFamilies, K rowKey) {
		mutator.deleteRow(columnFamilies, rowKey);
	}
	
	@Override
	public void discardMutations() {
		mutator.discardMutations();
	}
	
	@Override
	public void mergeShallow(MutationBatch other) {
		mutator.mergeShallow(other);
	}
	
	@Override
	public boolean isEmpty() {
		return mutator.isEmpty();
	}
	
	@Override
	public int getRowCount() {
		return mutator.getRowCount();
	}
	
	@Override
	public Map<ByteBuffer, Set<String>> getRowKeys() {
		return null;
	}
	
	@Override
	public MutationBatch pinToHost(Host host) {
		return mutator.pinToHost(host);
	}
	
	@Override
	public MutationBatch setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		return mutator.setConsistencyLevel(consistencyLevel);
	}
	
	@Override
	public MutationBatch withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		return mutator.withConsistencyLevel(consistencyLevel);
	}
	
	@Override
	public MutationBatch withRetryPolicy(RetryPolicy retry) {
		return mutator.withRetryPolicy(retry);
	}
	
	@Override
	public MutationBatch usingWriteAheadLog(WriteAheadLog manager) {
		return mutator.usingWriteAheadLog(manager);
	}
	@Override
	public MutationBatch lockCurrentTimestamp() {
		return mutator.lockCurrentTimestamp();
	}
	
	@Override
	public MutationBatch setTimeout(long timeout) {
		return mutator.setTimeout(timeout);
	}
	@Override
	public MutationBatch setTimestamp(long timestamp) {
		return mutator.setTimestamp(timestamp);
	}
	
	@Override
	public MutationBatch withTimestamp(long timestamp) {
		return mutator.withTimestamp(timestamp);
	}
	
	@Override
	public ByteBuffer serialize() throws Exception {
		return mutator.serialize();
	}
	
	@Override
	public void deserialize(ByteBuffer data) throws Exception {
		mutator.deserialize(data);		
	}
	
	@Override
	public OperationResult<Void> execute() throws ConnectionException {
		return mutator.execute();
	}

	@Override
	public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
		return mutator.executeAsync();
	}

	/**
	 * Wrapping the Column family mutator to "detect" puts on indexed columns
	 * 
	 * @author marcus
	 *
	 * @param <C> - the column to be indexed
	 * @param <K> - the column family's indexed row key
	 */
	class ThriftMutatorExt<C,K> extends AbstractColumnListMutation<C> implements ColumnListMutation<C> {

		ThriftColumnFamilyMutationImpl<C> impl;
		IndexCoordination coordination;
		ColumnFamily<?, C> columnFamily;
		MutationBatch mutator;
		K rowKey;
		
		public ThriftMutatorExt(MutationBatch mutator,ThriftColumnFamilyMutationImpl<C> impl,
				IndexCoordination coordination,ColumnFamily<?, C> columnFamily,K rowKey) {
			
			this.impl = impl;
			this.coordination = coordination;
			this.columnFamily = columnFamily;
			this.mutator = mutator;
			this.rowKey = rowKey;			
		}
		
		@Override
		public <V> ColumnListMutation<C> putColumn(C columnName, V value,
				Serializer<V> valueSerializer, Integer ttl) {
			
			//index first
			//if indexable
			IndexMappingKey<C> mappingKey = new IndexMappingKey<C>( columnFamily.getName(),columnName);
			if (coordination.indexExists(mappingKey)) {
			
				IndexMapping<C,V> mapping = coordination.get(mappingKey);
				coordination.modifying(mappingKey, value);			
				//check delta:
				try {
					//Get the index meta data to determine which index CF to store it in
					IndexMetadata<C, K> metaData = coordination.getMetaData(mappingKey );
					String indexCF = metaData.getIndexCFName();
					if (mapping.getOldValueofCol() == null ) {
						Index<C,V,K> ind = new IndexImpl<C, V, K>(mutator,columnFamily.getName(),indexCF);
						ind.insertIndex(columnName, value, rowKey);
					}else if (!mapping.getValueOfCol().equals(mapping.getOldValueofCol()) ) {
						Index<C,V,K> ind = new IndexImpl<C, V, K>(mutator,columnFamily.getName(),indexCF);
						ind.updateIndex(columnName, value, mapping.getOldValueofCol(), rowKey);
						
					} else {
						//no update required
					}
				}catch (ConnectionException e) {
					//TODO
					e.printStackTrace();
				}
			
			}
			//then modify
			impl.putColumn(columnName, value, valueSerializer, ttl);
			
			
			return this;
			
		}

		@Override
		public <V> ColumnListMutation<C> putColumnIfNotNull(C columnName,
				V value, Serializer<V> valueSerializer, Integer ttl) {
			//this won't support any 
			impl.putColumnIfNotNull(columnName, value, valueSerializer, ttl);
			
			return this;
		}


		@Override
		public <SC> ColumnListMutation<SC> withSuperColumn(
				ColumnPath<SC> superColumnPath) {
			
			//not supported
			return impl.withSuperColumn(superColumnPath);
			
			
		}


		@Override
		public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
			
			impl.putEmptyColumn(columnName, ttl);
			
			return this;
		}


		@Override
		public ColumnListMutation<C> incrementCounterColumn(C columnName,
				long amount) {
			impl.incrementCounterColumn(columnName, amount);
			
			return this;
		}


		@Override
		public ColumnListMutation<C> deleteColumn(C columnName) {
			impl.deleteColumn(columnName);
			
			return this;
		}


		@Override
		public ColumnListMutation<C> delete() {
			impl.delete();
			
			return this;
		}
				
	}
	
}


