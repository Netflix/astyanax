
package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Mutation;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Basic implementation of a mutation batch using the thrift data structures.
 * The thrift mutation data structure is,
 * 
 * Map of Keys -> Map of ColumnFamily -> MutationList
 * 
 * @author poberai
 * 
 */
public class CqlMutationBatchImpl implements MutationBatch {
    private static final long UNSET_TIMESTAMP = -1;
    
    private ChainedContext context; 
    
    protected long              timestamp;
    private ConsistencyLevel    consistencyLevel;
    private Clock               clock;
    private Host                pinnedHost;
    private RetryPolicy         retry;
    private WriteAheadLog       wal;
    private boolean             useAtomicBatch = false;
    private String              keyspace;
    private Cluster             cluster;

    //private Map<K, Map<String, List<Mutation>>> mutationMap = new HashMap<K, Map<String, List<Mutation>>>();
    private Map<KeyAndColumnFamily, CqlColumnFamilyMutationImpl<?,?>> rowLookup = Maps.newHashMap();
    
    private static class KeyAndColumnFamily<K> {
        private final String      columnFamily;
        private final K  key;
        
        public KeyAndColumnFamily(String columnFamily, K key) {
            this.columnFamily = columnFamily;
            this.key = key;
        }
        
        public int compareTo(Object obj) {
            if (obj instanceof KeyAndColumnFamily) {
                KeyAndColumnFamily<K> other = (KeyAndColumnFamily<K>)obj;
                int result = columnFamily.compareTo(other.columnFamily);
                if (result == 0) {
                    //result = key.compareTo(other.key);
                }
                return result;
            }
            return -1;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((columnFamily == null) ? 0 : columnFamily.hashCode());
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            KeyAndColumnFamily other = (KeyAndColumnFamily) obj;
            if (columnFamily == null) {
                if (other.columnFamily != null)
                    return false;
            } else if (!columnFamily.equals(other.columnFamily))
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            return true;
        }
    }
    
    public CqlMutationBatchImpl(ChainedContext context, Clock clock, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
    	
    	this.context = context;
    	context.rewindForRead(); 
    	this.cluster = context.getNext(Cluster.class);
    	this.keyspace = context.getNext(String.class);
    	
        this.clock            = clock;
        this.timestamp        = UNSET_TIMESTAMP;
        this.consistencyLevel = consistencyLevel;
        this.retry            = retry;
    }

    @Override
    public <K, C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
        Preconditions.checkNotNull(columnFamily, "columnFamily cannot be null");
        Preconditions.checkNotNull(rowKey, "Row key cannot be null");
        
        // Upon adding the first row into the mutation get the latest time from the clock
        if (timestamp == UNSET_TIMESTAMP)
            timestamp = clock.getCurrentTime();

        KeyAndColumnFamily kacf = new KeyAndColumnFamily(columnFamily.getName(), rowKey);
        CqlColumnFamilyMutationImpl<K, C> clm = (CqlColumnFamilyMutationImpl<K, C>) rowLookup.get(kacf);
        if (clm == null) {

//        	Map<K, List<Mutation>> innerMutationMap = mutationMap.get(rowKey);
//            if (innerMutationMap == null) {
//                innerMutationMap = Maps.newHashMap();
//                mutationMap.put(rowKey, innerMutationMap);
//            }
    
            //List<Mutation> innerMutationList = new ArrayList<Mutation>(); 
            		
//            		innerMutationMap.get(columnFamily.getName());
//            if (innerMutationList == null) {
//                innerMutationList = Lists.newArrayList();
//                innerMutationMap.put(columnFamily.getName(), innerMutationList);
//            }
            
            clm = new CqlColumnFamilyMutationImpl<K, C>(context.clone()
            											.add(columnFamily)
            											.add(rowKey), this.consistencyLevel, timestamp);
            rowLookup.put(kacf, clm);
        }
        return clm;
    }

    @Override
    public void discardMutations() {
        this.timestamp = UNSET_TIMESTAMP;
        //this.mutationMap.clear();
        this.rowLookup.clear();
    }

    @Override
    public <K> void deleteRow(Iterable<? extends ColumnFamily<K, ?>> columnFamilies, K rowKey) {
        for (ColumnFamily<K, ?> cf : columnFamilies) {
            withRow(cf, rowKey).delete();
        }
    }

    /**
     * Checks whether the mutation object contains rows. While the map may
     * contain row keys the row keys may not contain any mutations.
     * 
     * @return
     */
    @Override
    public boolean isEmpty() {
    	throw new NotImplementedException();
        //return mutationMap.isEmpty();
    }

    /**
     * Generate a string representation of the mutation with the following
     * syntax Key1: cf1: Mutation count cf2: Mutation count Key2: cf1: Mutation
     * count cf2: Mutation count
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CqlMutationBatch[");
//        boolean first = true;
//        for (Entry<ByteBuffer, Map<String, List<Mutation>>> row : mutationMap.entrySet()) {
//            if (!first)
//                sb.append(",");
//            sb.append(Hex.encodeHex(row.getKey().array())).append("(");
//            boolean first2 = true;
//            for (Entry<String, List<Mutation>> cf : row.getValue().entrySet()) {
//                if (!first2)
//                    sb.append(",");
//                sb.append(cf.getKey()).append(":").append(cf.getValue().size());
//                first2 = false;
//            }
//            first = false;
//            sb.append(")");
//        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public ByteBuffer serialize() throws Exception {
    	throw new NotImplementedException();
    }

    @Override
    public void deserialize(ByteBuffer data) throws Exception {
    	throw new NotImplementedException();
    }

    @Override
    public Map<ByteBuffer, Set<String>> getRowKeys() {
    	throw new NotImplementedException();
//        return Maps.transformEntries(mutationMap,
//                new EntryTransformer<ByteBuffer, Map<String, List<Mutation>>, Set<String>>() {
//                    @Override
//                    public Set<String> transformEntry(ByteBuffer key, Map<String, List<Mutation>> value) {
//                        return value.keySet();
//                    }
//                });
    }

    public Map<ByteBuffer, Map<String, List<Mutation>>> getMutationMap() {
    	throw new NotImplementedException();
    	//return mutationMap;
    }

    public void mergeShallow(MutationBatch other) {
    	throw new NotImplementedException();
//        if (!(other instanceof CqlMutationBatchImpl)) {
//            throw new UnsupportedOperationException();
//        }
//
//        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> otherRow : ((CqlMutationBatchImpl) other).mutationMap
//                .entrySet()) {
//            Map<String, List<Mutation>> thisRow = mutationMap.get(otherRow.getKey());
//            // Key not in the map
//            if (thisRow == null) {
//                mutationMap.put(otherRow.getKey(), otherRow.getValue());
//            }
//            else {
//                for (Map.Entry<String, List<Mutation>> otherCf : otherRow.getValue().entrySet()) {
//                    List<Mutation> thisCf = thisRow.get(otherCf.getKey());
//                    // Column family not in the map
//                    if (thisCf == null) {
//                        thisRow.put(otherCf.getKey(), otherCf.getValue());
//                    }
//                    else {
//                        thisCf.addAll(otherCf.getValue());
//                    }
//                }
//            }
//        }
    }

    @Override
    public int getRowCount() {
    	throw new NotImplementedException();
    	//return mutationMap.size();
    }

    @Override
    public MutationBatch setTimeout(long timeout) {
        return this;
    }

    @Override
    public MutationBatch setTimestamp(long timestamp) {
        return withTimestamp(timestamp);
    }
    
    @Override
    public MutationBatch withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
    
    @Override
    public MutationBatch lockCurrentTimestamp() {
        this.timestamp = clock.getCurrentTime();
        return this;
    }
    
    @Override
    public MutationBatch setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }
    
    @Override
    public MutationBatch withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    public MutationBatch pinToHost(Host host) {
        this.pinnedHost = host;
        return this;
    }
    
    @Override
    public MutationBatch withRetryPolicy(RetryPolicy retry) {
        this.retry = retry;
        return this;
    }

    @Override
    public MutationBatch usingWriteAheadLog(WriteAheadLog manager) {
        this.wal = manager;
        return this;
    }
    
    @Override
    public MutationBatch withAtomicBatch(boolean condition) {
        useAtomicBatch = condition;
        return this;
    }

    public boolean useAtomicBatch() {
        return useAtomicBatch;
    }
    
    public Host getPinnedHost() {
        return this.pinnedHost;
    }

    public RetryPolicy getRetryPolicy() {
        return this.retry;
    }
    
    public WriteAheadLog getWriteAheadLog() {
        return this.wal;
    }

	@Override
	public OperationResult<Void> execute() throws ConnectionException {
		
		BoundStatement statement = getTotalStatement();
		ResultSet rs = cluster.connect().execute(statement);
		return new CqlOperationResultImpl<Void>(rs, null);
	}

	@Override
	public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {

		if (rowLookup.values().size() > 1) {
			throw new NotImplementedException();
		}
		
		BoundStatement statement = getTotalStatement();
		ResultSetFuture rsFuture = cluster.connect().executeAsync(statement);
		return new AsyncOperationResult<Void>(rsFuture) {
			@Override
			public OperationResult<Void> getOperationResult(ResultSet rs) {
				return new CqlOperationResultImpl<Void>(rs, null);
			}
		};
	}
	
	private BoundStatement getTotalStatement() {
		BatchedStatements statements = new BatchedStatements();
		
		for (CqlColumnFamilyMutationImpl<?, ?> cfMutation : rowLookup.values()) {
			statements.addBatch(cfMutation.getBatch());
		}
		
		return statements.getBoundStatement(cluster, useAtomicBatch);
	}

	public String getKeyspace() {
		return keyspace;
	}

	public Cluster getCluster() {
		return cluster;
	}
}