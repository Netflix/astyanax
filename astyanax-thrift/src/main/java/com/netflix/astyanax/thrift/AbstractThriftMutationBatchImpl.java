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

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;

import org.apache.cassandra.thrift.Cassandra.batch_mutate_args;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.commons.codec.binary.Hex;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TIOStreamTransport;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;

/**
 * Basic implementation of a mutation batch using the thrift data structures.
 * The thrift mutation data structure is,
 * 
 * Map of Keys -> Map of ColumnFamily -> MutationList
 * 
 * @author elandau
 * 
 */
public abstract class AbstractThriftMutationBatchImpl implements MutationBatch {
    private static final long UNSET_TIMESTAMP = -1;
    
    protected long              timestamp;
    private ConsistencyLevel    consistencyLevel;
    private Clock               clock;
    private Host                pinnedHost;
    private RetryPolicy         retry;
    private WriteAheadLog       wal;
    private boolean             useAtomicBatch = false;

    private Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = Maps.newLinkedHashMap();
    private Map<KeyAndColumnFamily, ColumnListMutation<?>> rowLookup = Maps.newHashMap();
    
    private static class KeyAndColumnFamily {
        private final String      columnFamily;
        private final ByteBuffer  key;
        
        public KeyAndColumnFamily(String columnFamily, ByteBuffer key) {
            this.columnFamily = columnFamily;
            this.key = key;
        }
        
        public int compareTo(Object obj) {
            if (obj instanceof KeyAndColumnFamily) {
                KeyAndColumnFamily other = (KeyAndColumnFamily)obj;
                int result = columnFamily.compareTo(other.columnFamily);
                if (result == 0) {
                    result = key.compareTo(other.key);
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
    
    public AbstractThriftMutationBatchImpl(Clock clock, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
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

        ByteBuffer bbKey = columnFamily.getKeySerializer().toByteBuffer(rowKey);
        if (!bbKey.hasRemaining()) {
            throw new RuntimeException("Row key cannot be empty");
        }
        
        KeyAndColumnFamily kacf = new KeyAndColumnFamily(columnFamily.getName(), bbKey);
        ColumnListMutation<C> clm = (ColumnListMutation<C>) rowLookup.get(kacf);
        if (clm == null) {
            Map<String, List<Mutation>> innerMutationMap = mutationMap.get(bbKey);
            if (innerMutationMap == null) {
                innerMutationMap = Maps.newHashMap();
                mutationMap.put(bbKey, innerMutationMap);
            }
    
            List<Mutation> innerMutationList = innerMutationMap.get(columnFamily.getName());
            if (innerMutationList == null) {
                innerMutationList = Lists.newArrayList();
                innerMutationMap.put(columnFamily.getName(), innerMutationList);
            }
            
            clm = new ThriftColumnFamilyMutationImpl<C>(timestamp, innerMutationList, columnFamily.getColumnSerializer());
            rowLookup.put(kacf, clm);
        }
        return clm;
    }

    @Override
    public void discardMutations() {
        this.timestamp = UNSET_TIMESTAMP;
        this.mutationMap.clear();
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
        return mutationMap.isEmpty();
    }

    /**
     * Generate a string representation of the mutation with the following
     * syntax Key1: cf1: Mutation count cf2: Mutation count Key2: cf1: Mutation
     * count cf2: Mutation count
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

    @Override
    public ByteBuffer serialize() throws Exception {
        if (mutationMap.isEmpty()) {
            throw new Exception("Mutation is empty");
        }

        ByteBufferOutputStream out       = new ByteBufferOutputStream();
        TIOStreamTransport     transport = new TIOStreamTransport(out);
        batch_mutate_args      args      = new batch_mutate_args();
        args.setMutation_map(mutationMap);

        try {
            args.write(new TBinaryProtocol(transport));
        }
        catch (TException e) {
            throw ThriftConverter.ToConnectionPoolException(e);
        }

        return out.getByteBuffer();
    }

    @Override
    public void deserialize(ByteBuffer data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data.array());
        TIOStreamTransport transport = new TIOStreamTransport(in);
        batch_mutate_args args = new batch_mutate_args();

        try {
            TBinaryProtocol bp = new TBinaryProtocol(transport);
            bp.setReadLength(data.remaining());
            args.read(bp);
            mutationMap = args.getMutation_map();
        }
        catch (TException e) {
            throw ThriftConverter.ToConnectionPoolException(e);
        }
    }

    @Override
    public Map<ByteBuffer, Set<String>> getRowKeys() {
        return Maps.transformEntries(mutationMap,
                new EntryTransformer<ByteBuffer, Map<String, List<Mutation>>, Set<String>>() {
                    @Override
                    public Set<String> transformEntry(ByteBuffer key, Map<String, List<Mutation>> value) {
                        return value.keySet();
                    }
                });
    }

    public Map<ByteBuffer, Map<String, List<Mutation>>> getMutationMap() {
        return mutationMap;
    }

    public void mergeShallow(MutationBatch other) {
        if (!(other instanceof AbstractThriftMutationBatchImpl)) {
            throw new UnsupportedOperationException();
        }

        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> otherRow : ((AbstractThriftMutationBatchImpl) other).mutationMap
                .entrySet()) {
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

}
