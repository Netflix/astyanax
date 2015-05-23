package com.netflix.astyanax.cql.writes;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

public abstract class AbstractMutationBatchImpl implements MutationBatch {

	//private static final long UNSET_TIMESTAMP = -1;

	protected Long              timestamp = null; // UNSET_TIMESTAMP 
	protected ConsistencyLevel    consistencyLevel;
	protected Clock               clock;
	protected Host                pinnedHost;
	protected RetryPolicy         retry;
	protected WriteAheadLog       wal;
	protected boolean             useAtomicBatch = false;
	protected String              keyspace; 

	protected Map<ByteBuffer, Map<String, ColumnListMutation<?>>> mutationMap = Maps.newLinkedHashMap();
	protected Map<KeyAndColumnFamily, ColumnListMutation<?>> rowLookup = Maps.newHashMap();

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

	public AbstractMutationBatchImpl(Clock clock, ConsistencyLevel consistencyLevel, RetryPolicy retry) {
		this.clock            = clock;
		this.timestamp        = null; //UNSET_TIMESTAMP;
		this.consistencyLevel = consistencyLevel;
		this.retry            = retry;
	}

	@Override
	public <K, C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
		Preconditions.checkNotNull(columnFamily, "columnFamily cannot be null");
		Preconditions.checkNotNull(rowKey, "Row key cannot be null");

		ByteBuffer bbKey = columnFamily.getKeySerializer().toByteBuffer(rowKey);
		if (!bbKey.hasRemaining()) {
			throw new RuntimeException("Row key cannot be empty");
		}

		KeyAndColumnFamily kacf = new KeyAndColumnFamily(columnFamily.getName(), bbKey);
		ColumnListMutation<C> clm = (ColumnListMutation<C>) rowLookup.get(kacf);
		if (clm == null) {
			Map<String, ColumnListMutation<?>> innerMutationMap = mutationMap.get(bbKey);
			if (innerMutationMap == null) {
				innerMutationMap = Maps.newHashMap();
				mutationMap.put(bbKey, innerMutationMap);
			}

			ColumnListMutation<?> innerMutationList = innerMutationMap.get(columnFamily.getName());
			if (innerMutationList == null) {
				innerMutationList = createColumnListMutation(keyspace, columnFamily, rowKey);
				innerMutationMap.put(columnFamily.getName(), innerMutationList);
			}

			rowLookup.put(kacf, innerMutationList);
			clm = (ColumnListMutation<C>) innerMutationList;
		}
		return clm;
	}

	public abstract <K,C> ColumnListMutation<C> createColumnListMutation(String keyspace, ColumnFamily<K,C> cf, K rowKey); 

	@Override
	public void discardMutations() {
		this.timestamp = null; //UNSET_TIMESTAMP;
		this.mutationMap.clear();
		this.rowLookup.clear();
		this.withCaching(false); // TURN Caching off.
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
	 * syntax Key1: [cf1, cf2],  Key2: [cf1, cf3]
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("MutationBatch[");
		boolean first = true;
		for (Entry<ByteBuffer, Map<String, ColumnListMutation<?>>> row : mutationMap.entrySet()) {
			if (!first)
				sb.append(",");
			sb.append(Hex.encodeHex(row.getKey().array()));
			sb.append(row.getValue().entrySet().toString());
		}
		sb.append("]");
		return sb.toString();
	}

	@Override
	public ByteBuffer serialize() throws Exception {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public void deserialize(ByteBuffer data) throws Exception {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Map<ByteBuffer, Set<String>> getRowKeys() {
		return Maps.transformEntries(mutationMap,
				new EntryTransformer<ByteBuffer, Map<String, ColumnListMutation<?>>, Set<String>>() {
			@Override
			public Set<String> transformEntry(ByteBuffer key, Map<String, ColumnListMutation<?>> value) {
				return value.keySet();
			}
		});
	}

	public Map<ByteBuffer, Map<String, ColumnListMutation<?>>> getMutationMap() {
		return mutationMap;
	}

	public void mergeShallow(MutationBatch other) {
		if (!(other instanceof AbstractMutationBatchImpl)) {
			throw new UnsupportedOperationException();
		}

		for (Map.Entry<ByteBuffer, Map<String, ColumnListMutation<?>>> otherRow : ((AbstractMutationBatchImpl) other).mutationMap
				.entrySet()) {
			Map<String, ColumnListMutation<?>> thisRow = mutationMap.get(otherRow.getKey());
			// Key not in the map
			if (thisRow == null) {
				mutationMap.put(otherRow.getKey(), otherRow.getValue());
			}
			else {
				for (Map.Entry<String, ColumnListMutation<?>> otherCf : otherRow.getValue().entrySet()) {
					ColumnListMutation<?> thisCf = thisRow.get(otherCf.getKey());
					// Column family not in the map
					if (thisCf == null) {
						thisRow.put(otherCf.getKey(), otherCf.getValue());
					}
					else {
						mergeColumnListMutation(otherCf.getValue(), thisCf);
					}
				}
			}
		}
	}

	public abstract void mergeColumnListMutation(ColumnListMutation<?> from, ColumnListMutation<?> to);

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
		this.retry = retry.duplicate();
		return this;
	}

	@Override
	public MutationBatch usingWriteAheadLog(WriteAheadLog manager) {
		throw new UnsupportedOperationException("Operation not supported. ");
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
