package com.netflix.astyanax.index;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.thrift.ThriftColumnFamilyMutationImpl;

public class HCMutationBatchImpl implements IndexedMutationBatch {

	IndexContext indexcontext;
	
	@Override
	public <K, C> ColumnListMutation<C> withIndexedRow(
			MutationBatch currentBatch, ColumnFamily<K, C> columnFamily,
			K rowKey) {
		
				
		//wrap and return wrapping
		ThriftColumnFamilyMutationImpl<C> impl =  (ThriftColumnFamilyMutationImpl<C>)currentBatch.withRow(columnFamily, rowKey);
				
		
		ThriftMutatorExt<C> cfMutatorWrapper = new ThriftMutatorExt<C>(impl); 
		
		return cfMutatorWrapper;
		
	}

	/**
	 * Wrapping the Column family mutator to "detect" puts on indexed columns
	 * 
	 * @author marcus
	 *
	 * @param <C>
	 */
	class ThriftMutatorExt<C> implements ColumnListMutation<C> {

		ThriftColumnFamilyMutationImpl<C> impl;
		
		public ThriftMutatorExt(ThriftColumnFamilyMutationImpl<C> impl) {
			this.impl = impl;
		}

		@Override
		public <V> ColumnListMutation<C> putColumn(C columnName, V value,
				Serializer<V> valueSerializer, Integer ttl) {
			
			return impl.putColumn(columnName, value, valueSerializer, ttl);
			
		}

		@Override
		public <V> ColumnListMutation<C> putColumnIfNotNull(C columnName,
				V value, Serializer<V> valueSerializer, Integer ttl) {
			
			return impl.putColumnIfNotNull(columnName, value, valueSerializer, ttl);
		}

		@Override
		public <SC> ColumnListMutation<SC> withSuperColumn(
				ColumnPath<SC> superColumnPath) {
			
			return impl.withSuperColumn(superColumnPath);
			
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, String value,
				Integer ttl) {
			
			return impl.putColumnIfNotNull(columnName, value,  ttl);
			
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, String value) {
			
			return impl.putColumnIfNotNull(columnName, value );
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				String value, Integer ttl) {
			return impl.putColumnIfNotNull(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				String value) {
			return impl.putColumnIfNotNull(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, byte[] value,
				Integer ttl) {
			return impl.putColumn(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, byte[] value) {
			return impl.putColumn(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				byte[] value, Integer ttl) {
			return impl.putColumnIfNotNull(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				byte[] value) {
			return impl.putColumnIfNotNull(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, int value,
				Integer ttl) {
			return impl.putColumn(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, int value) {
			
			return impl.putColumn(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Integer value, Integer ttl) {
			return impl.putColumnIfNotNull(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Integer value) {
			return impl.putColumnIfNotNull(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, long value,
				Integer ttl) {
			return impl.putColumn(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, long value) {
			return impl.putColumn(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Long value, Integer ttl) {
			return impl.putColumnIfNotNull(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value) {
			return impl.putColumnIfNotNull(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, boolean value,
				Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, boolean value) {
			return impl.putColumn(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Boolean value, Integer ttl) {
			return impl.putColumnIfNotNull(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Boolean value) {
			return impl.putColumnIfNotNull(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value,
				Integer ttl) {
			return impl.putColumn(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value) {
			return impl.putColumn(columnName, value);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				ByteBuffer value, Integer ttl) {
			return impl.putColumnIfNotNull(columnName, value,ttl);
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				ByteBuffer value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, Date value,
				Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, Date value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Date value, Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, float value,
				Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, float value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Float value, Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Float value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, double value,
				Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, double value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Double value, Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				Double value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, UUID value,
				Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumn(C columnName, UUID value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName,
				UUID value, Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> putEmptyColumn(C columnName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> incrementCounterColumn(C columnName,
				long amount) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> deleteColumn(C columnName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> setTimestamp(long timestamp) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> delete() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnListMutation<C> setDefaultTtl(Integer ttl) {
			// TODO Auto-generated method stub
			return null;
		}
		
		
	}
	
}


