package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ExecutionHelper;

import org.apache.cassandra.thrift.Mutation;
import org.apache.commons.codec.binary.Hex;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;

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

	protected ConsistencyLevel consistencyLevel;
	protected long timeout;
	protected final Clock clock;
	
    private Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap 
    	= new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
    
    public AbstractThriftMutationBatchImpl(Clock clock, ConsistencyLevel consistencyLevel, int timeout) {
    	this.consistencyLevel = consistencyLevel;
    	this.timeout = timeout;
    	this.clock = clock;
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
        this.mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
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
	    	innerMutationMap = new HashMap<String, List<Mutation>>();
	    	mutationMap.put(colFamily.getKeySerializer().toByteBuffer(key), 
	    					innerMutationMap);
	    }
	    
	    List<Mutation> innerMutationList = innerMutationMap.get(colFamily.getName());
	    if (innerMutationList == null) {
	    	innerMutationList = new ArrayList<Mutation>();
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
	public MutationBatch setConsistencyLevel(
			ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public MutationBatch setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}


    @Override
    public OperationResult<Void> execute() throws ConnectionException {
        return ExecutionHelper.blockingExecute(this);
    }
}
