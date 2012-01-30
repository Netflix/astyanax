/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.clock.ConstantClock;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ExecutionHelper;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.commons.codec.binary.Hex;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Basic implementation of a mutation batch using the thrift data structures.
 * The thrift mutation data structure is,
 * 
 * 		Map of Keys -> Map of ColumnFamily -> MutationList
 * 
 * @author elandau
 *
 */
public abstract class AbstractThriftMutationBatchImpl implements MutationBatch {

	protected Clock clock;
	protected Clock sysClock;
	
	enum ColumnType {
		NULL,
		STANDARD,
		COUNTER,
		DELETION,
		ROW_DELETION,
	};
	
    private Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = Maps.newHashMap();
    
    public AbstractThriftMutationBatchImpl(Clock clock) {
    	this.clock = this.sysClock = clock;
    }
    
    @Override
	public <K,C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
		return new ThriftColumnFamilyMutationImpl<C>(
				clock,
				getColumnFamilyMutationList(columnFamily, rowKey),
				columnFamily.getColumnSerializer());
	}

    @Override
    public void discardMutations() {
        this.mutationMap = Maps.newHashMap();
    }
    
	@Override
	public <K> void deleteRow(Collection<ColumnFamily<K, ?>> columnFamilies, K rowKey)
	{
		for (ColumnFamily<K, ?> cf : columnFamilies) {
			withRow(cf, rowKey).delete();
		}
	}
	
	/**
	 * Checks whether the mutation object contains rows.  While the map
	 * may contain row keys the row keys may not contain any mutations.
	 * @return
	 */
	@Override
	public boolean isEmpty() {
	    return mutationMap.isEmpty();
	}	
	
	/**
	 * Generate a string representation of the mutation with the following syntax
	 * 	Key1:
	 * 		cf1: Mutation count
	 * 		cf2: Mutation count
	 *  Key2:
	 *  	cf1: Mutation count
	 *  	cf2: Mutation count
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ThriftMutationBatch[");
		boolean first = true;
		for (Entry<ByteBuffer, Map<String, List<Mutation>>> row : mutationMap.entrySet()) {
			if (!first)
				sb.append(",");
			sb.append(Hex.encodeHex(row.getKey().array())).append("(");
			boolean first2 = true;
			for (Entry<String, List<Mutation>> cf : row.getValue().entrySet()) {
				if (!first2) 
					sb.append(",");
				sb.append(cf.getKey()).append(":").append(cf.getValue().size());
				first2 = false;
			}
			first = false;
			sb.append(")");
		}
		sb.append("]");
		return sb.toString();
	}
	
	protected ByteBuffer toByteBuffer() throws Exception {
		if (mutationMap.isEmpty()) {
			throw new Exception("Mutation is empty");
		}
		if (mutationMap.size() > 1) {
			throw new Exception("Transaction mutation limited to one key only");
		}
		
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		
		Map<String, List<Mutation>> cfs = mutationMap.entrySet().iterator().next().getValue();
		out.write(IntegerSerializer.get().toByteBuffer(cfs.size()));
		for (Entry<String, List<Mutation>> cf : cfs.entrySet()) {
			if (cf.getValue().size() > 0) {
				out.write(StringSerializer.get().toByteBuffer(cf.getKey()));
				out.write(IntegerSerializer.get().toByteBuffer(cf.getValue().size()));
				for (Mutation m : cf.getValue()) {
					if (m.isSetDeletion()) {
					}
					else {
						ColumnOrSuperColumn cosc = m.getColumn_or_supercolumn();
						if (cosc.isSetColumn()) {
							out.write(ColumnType.STANDARD.ordinal());
							out.write(cosc.getColumn().bufferForName().duplicate());
							out.write(cosc.getColumn().bufferForValue().duplicate());
							out.write(LongSerializer.get().toByteBuffer(cosc.getColumn().getTimestamp()));
							out.write(cosc.getColumn().getTtl());
						}
						else if (cosc.isSetCounter_column()) {
							out.write(ColumnType.STANDARD.ordinal());
							out.write(cosc.getCounter_column().bufferForName().duplicate());
							out.write(LongSerializer.get().toByteBuffer(cosc.getCounter_column().getValue()));
						}
						else if (cosc.isSetSuper_column() || cosc.isSetCounter_super_column()) {
							throw new Exception("Counter column not supported");
						}
						else {
							out.write(ColumnType.NULL.ordinal());
						}
					}
				}
			}
		}
		
		return out.getByteBuffer();
	}
	
	public static AbstractThriftMutationBatchImpl fromByteBuffer(ByteBuffer blob) {
		int cfCount = IntegerSerializer.get().fromByteBuffer(blob);
		while (cfCount-- > 0) {
			String cfName = StringSerializer.get().fromByteBuffer(blob);
			int mCount = IntegerSerializer.get().fromByteBuffer(blob);
		}
		// TODO
		return null;
	}
	
	/**
	 * Get or add a column family mutation to this row
	 * 
	 * @param colFamily
	 * @return
	 */
	private <K, C> List<Mutation> getColumnFamilyMutationList(ColumnFamily<K, C> colFamily, K key) {
	    Map<String, List<Mutation>> innerMutationMap = mutationMap.get(
	    		colFamily.getKeySerializer().toByteBuffer(key));
	    if (innerMutationMap == null) {
	    	innerMutationMap = Maps.newHashMap();
	    	mutationMap.put(colFamily.getKeySerializer().toByteBuffer(key), 
	    					innerMutationMap);
	    }
	    
	    List<Mutation> innerMutationList = innerMutationMap.get(colFamily.getName());
	    if (innerMutationList == null) {
	    	innerMutationList = Lists.newArrayList();
	    	innerMutationMap.put(colFamily.getName(), innerMutationList);
	    }
	    return innerMutationList;
	}
    
	protected Map<ByteBuffer,Map<String,List<Mutation>>> getMutationMap() {
	    return mutationMap;
	}
	
	public void mergeShallow(MutationBatch other) {
		if (!(other instanceof AbstractThriftMutationBatchImpl)) {
			throw new UnsupportedOperationException();
		}
		
		for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> otherRow : ((AbstractThriftMutationBatchImpl)other).mutationMap.entrySet()) {
			Map<String, List<Mutation>> thisRow = mutationMap.get(otherRow.getKey());
			// Key not in the map
			if (thisRow == null) {
				mutationMap.put(otherRow.getKey(), otherRow.getValue());
			}
			else {
				for (Map.Entry<String, List<Mutation>> otherCf : otherRow.getValue().entrySet()) {
					List<Mutation> thisCf = thisRow.get(otherCf.getKey());
					// Column family not in the map
					if (thisCf == null) {
						thisRow.put(otherCf.getKey(), otherCf.getValue());
					}
					else {
						thisCf.addAll(otherCf.getValue());
					}
				}
			}
		}
	}

	@Override
	public int getRowCount() {
		return mutationMap.size();
	}
	
	@Override
	public MutationBatch setTimeout(long timeout) {
		return this;
	}

	@Override
	public MutationBatch setTimestamp(long timestamp) {
		this.clock = new ConstantClock(timestamp);
		return this;
	}
	
	@Override
	public MutationBatch lockCurrentTimestamp() {
		setTimestamp(this.sysClock.getCurrentTime());
		return this;
	}

    @Override
    public OperationResult<Void> execute() throws ConnectionException {
        return ExecutionHelper.blockingExecute(this);
    }
}
