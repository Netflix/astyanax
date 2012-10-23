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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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

    protected long timestamp;
    private ConsistencyLevel consistencyLevel;
    private Clock clock;
    private Host pinnedHost;
    private RetryPolicy retry;
    private WriteAheadLog wal;

    private Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = Maps.newLinkedHashMap();

    public AbstractThriftMutationBatchImpl(Clock clock, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
        this.clock = clock;
        this.timestamp = clock.getCurrentTime();
        this.consistencyLevel = consistencyLevel;
        this.retry = retry;
    }

    @Override
    public <K, C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
        if (clock != null && mutationMap.isEmpty())
            this.timestamp = clock.getCurrentTime();

        return new ThriftColumnFamilyMutationImpl<C>(timestamp, getColumnFamilyMutationList(columnFamily, rowKey),
                columnFamily.getColumnSerializer());
    }

    @Override
    public void discardMutations() {
        this.mutationMap = Maps.newHashMap();
    }

    @Override
    public <K> void deleteRow(Collection<ColumnFamily<K, ?>> columnFamilies, K rowKey) {
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

        ByteBufferOutputStream out = new ByteBufferOutputStream();
        TIOStreamTransport transport = new TIOStreamTransport(out);
        batch_mutate_args args = new batch_mutate_args();
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

    /**
     * Get or add a column family mutation to this row
     * 
     * @param colFamily
     * @return
     */
    private <K, C> List<Mutation> getColumnFamilyMutationList(ColumnFamily<K, C> colFamily, K key) {
        Map<String, List<Mutation>> innerMutationMap = mutationMap.get(colFamily.getKeySerializer().toByteBuffer(key));
        if (innerMutationMap == null) {
            innerMutationMap = Maps.newHashMap();
            mutationMap.put(colFamily.getKeySerializer().toByteBuffer(key), innerMutationMap);
        }

        List<Mutation> innerMutationList = innerMutationMap.get(colFamily.getName());
        if (innerMutationList == null) {
            innerMutationList = Lists.newArrayList();
            innerMutationMap.put(colFamily.getName(), innerMutationList);
        }
        return innerMutationList;
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
        this.clock = null;
        this.timestamp = timestamp;
        return this;
    }
    
    @Override
    public MutationBatch withTimestamp(long timestamp) {
        this.clock = null;
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public MutationBatch lockCurrentTimestamp() {
        this.timestamp = this.clock.getCurrentTime();
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
