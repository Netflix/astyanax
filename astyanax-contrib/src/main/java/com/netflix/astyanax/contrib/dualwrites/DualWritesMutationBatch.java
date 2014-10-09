package com.netflix.astyanax.contrib.dualwrites;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Class that implements the {@link MutationBatch} interface and acts as a dual router for capturing all the dual writes. 
 * Note that it purely maintains state in 2 separate MutationBatch objects, each corresponding to the source of destination keyspace / mutation batches.
 * 
 * It also tracks state of what row keys are being added to what column families. This is useful for reporting data when the dual writes fail partially,
 * and hence that metadata can be communicated to some {@link FailedWritesLogger} to be dealt with accordingly. 
 * 
 * @author poberai
 *
 * @param <C>
 */
public class DualWritesMutationBatch implements MutationBatch {

    private final DualKeyspaceMetadata dualKeyspaceMetadata;
	private final MutationBatch primary; 
	private final MutationBatch secondary; 
	private final DualWritesStrategy writeExecutionStrategy;
	
	
	private final AtomicReference<List<WriteMetadata>> writeMetada = new AtomicReference<List<WriteMetadata>>(new ArrayList<WriteMetadata>());
	
	public DualWritesMutationBatch(DualKeyspaceMetadata dualKSMetadata, 
	        MutationBatch primaryMB, MutationBatch secondaryMB, DualWritesStrategy strategy) {
	    this.dualKeyspaceMetadata = dualKSMetadata;
		this.primary = primaryMB;
		this.secondary = secondaryMB;
		this.writeExecutionStrategy = strategy;
	}

	public MutationBatch getPrimary() {
	    return primary;
	}
	
	public MutationBatch getSecondary() {
	    return secondary;
	}
	
	@Override
	public OperationResult<Void> execute() throws ConnectionException {
	    return writeExecutionStrategy.wrapExecutions(primary, secondary, writeMetada.get()).execute();
	}

	@Override
	public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
        return writeExecutionStrategy.wrapExecutions(primary, secondary, writeMetada.get()).executeAsync();
	}

	@Override
	public <K, C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
	    
	    writeMetada.get().add(new WriteMetadata(dualKeyspaceMetadata, columnFamily.getName(), rowKey.toString()));
	    
        ColumnListMutation<C> clmPrimary = primary.withRow(columnFamily, rowKey);
        ColumnListMutation<C> clmSecondary = secondary.withRow(columnFamily, rowKey);
		return new DualWritesColumnListMutation<C>(clmPrimary, clmSecondary);
	}

	@Override
	public <K> void deleteRow(Iterable<? extends ColumnFamily<K, ?>> columnFamilies, K rowKey) {

	    for (ColumnFamily<K, ?> cf : columnFamilies) {
	        writeMetada.get().add(new WriteMetadata(dualKeyspaceMetadata, cf.getName(), rowKey.toString()));
	    }
	       
	    primary.deleteRow(columnFamilies, rowKey);
		secondary.deleteRow(columnFamilies, rowKey);
	}

	@Override
	public void discardMutations() {
		primary.discardMutations();
		secondary.discardMutations();
		writeMetada.set(new ArrayList<WriteMetadata>());
	}

	@Override
	public void mergeShallow(MutationBatch other) {
		primary.mergeShallow(other);
		secondary.mergeShallow(other);
	}

	@Override
	public boolean isEmpty() {
		return primary.isEmpty();
	}

	@Override
	public int getRowCount() {
		return primary.getRowCount();
	}

	@Override
	public Map<ByteBuffer, Set<String>> getRowKeys() {
		return primary.getRowKeys();
	}

	@Override
	public MutationBatch pinToHost(Host host) {
		primary.pinToHost(host);
		secondary.pinToHost(host);
		return this;
	}

	@Override
	public MutationBatch setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		primary.setConsistencyLevel(consistencyLevel);
		secondary.setConsistencyLevel(consistencyLevel);
		return this;
	}

	@Override
	public MutationBatch withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		primary.withConsistencyLevel(consistencyLevel);
		secondary.withConsistencyLevel(consistencyLevel);
		return this;
	}

	@Override
	public MutationBatch withRetryPolicy(RetryPolicy retry) {
		primary.withRetryPolicy(retry);
		secondary.withRetryPolicy(retry);
		return this;
	}

	@Override
	public MutationBatch usingWriteAheadLog(WriteAheadLog manager) {
		primary.usingWriteAheadLog(manager);
		return this;
	}

	@Override
	public MutationBatch lockCurrentTimestamp() {
		primary.lockCurrentTimestamp();
		secondary.lockCurrentTimestamp();
		return this;
	}

	@SuppressWarnings("deprecation")
	@Override
	public MutationBatch setTimeout(long timeout) {
		primary.setTimeout(timeout);
		secondary.setTimeout(timeout);
		return this;
	}

	@Override
	public MutationBatch setTimestamp(long timestamp) {
		primary.setTimestamp(timestamp);
		secondary.setTimestamp(timestamp);
		return this;
	}

	@Override
	public MutationBatch withTimestamp(long timestamp) {
		primary.withTimestamp(timestamp);
		secondary.withTimestamp(timestamp);
		return this;
	}

	@Override
	public MutationBatch withAtomicBatch(boolean condition) {
		primary.withAtomicBatch(condition);
		secondary.withAtomicBatch(condition);
		return this;
	}

	@Override
	public ByteBuffer serialize() throws Exception {
		secondary.serialize();
		return primary.serialize();
	}

	@Override
	public void deserialize(ByteBuffer data) throws Exception {
		ByteBuffer clone = clone(data);
		primary.deserialize(data);
		secondary.deserialize(clone);
	}

	@Override
	public MutationBatch withCaching(boolean condition) {
		primary.withCaching(condition);
		secondary.withCaching(condition);
		return this;
	}
	
	private static ByteBuffer clone(ByteBuffer original) {
		ByteBuffer clone = ByteBuffer.allocate(original.capacity());
		original.rewind();//copy from the beginning
		clone.put(original);
		original.rewind();
		clone.flip();
		return clone;
	}
}
