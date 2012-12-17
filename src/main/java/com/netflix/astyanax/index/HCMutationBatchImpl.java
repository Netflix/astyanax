package com.netflix.astyanax.index;

import com.netflix.astyanax.AbstractColumnListMutation;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.thrift.ThriftColumnFamilyMutationImpl;

public class HCMutationBatchImpl implements IndexedMutationBatch {

	private IndexCoordination indexcoorindator;
	
	//TODO perhaps a constructor with the mutator is called for??
	
	public HCMutationBatchImpl() {
		this.indexcoorindator = IndexCoordinationFactory.getIndexContext();
	}
	public HCMutationBatchImpl(IndexCoordination coordinator) {
		this.indexcoorindator = coordinator;
	}
	@Override
	public <K, C> ColumnListMutation<C> withIndexedRow(
			MutationBatch currentBatch, ColumnFamily<K, C> columnFamily,
			K rowKey) {
		
				
		//wrap and return wrapping
		//this might throw if its the super column implementation :(
		ThriftColumnFamilyMutationImpl<C> impl =  (ThriftColumnFamilyMutationImpl<C>)currentBatch.withRow(columnFamily, rowKey);
				
		
		ThriftMutatorExt<C,K> cfMutatorWrapper = new ThriftMutatorExt<C,K>(currentBatch,impl,indexcoorindator,columnFamily, rowKey); 
		
		return cfMutatorWrapper;
		
	}

	
	public IndexCoordination getIndexcoorindator() {
		return indexcoorindator;
	}
	public void setIndexcoorindator(IndexCoordination indexcoorindator) {
		this.indexcoorindator = indexcoorindator;
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
					if (mapping.getOldValueofCol() == null ) {
						Index<C,V,K> ind = new IndexImpl<C, V, K>(mutator,columnFamily.getName());
						ind.insertIndex(columnName, value, rowKey);
					}else if (!mapping.getValueOfCol().equals(mapping.getOldValueofCol()) ) {
						Index<C,V,K> ind = new IndexImpl<C, V, K>(mutator,columnFamily.getName());
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


