package com.netflix.astyanax.thrift;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.AllRowsQuery;

public abstract class AbstractThriftAllRowsQueryImpl<K,C> implements AllRowsQuery<K,C> {
	
	protected SlicePredicate predicate = new SlicePredicate().setSlice_range(ThriftUtils.RANGE_ALL);
	protected KeyRange range = new KeyRange().setCount(100).setStart_token("0").setEnd_token("0");
	
	private ColumnFamily<K, C> columnFamily;
	private boolean repeatLastToken = true;
	private final RandomPartitioner partitioner = new RandomPartitioner();

	public AbstractThriftAllRowsQueryImpl(ColumnFamily<K, C> columnFamily) {
		this.columnFamily = columnFamily;
	}
	
	@Override 
	public AllRowsQuery<K, C> withColumnSlice(C... columns) {
    	predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(Arrays.asList(columns))).setSlice_rangeIsSet(false);
		return this;
	}

	@Override
	public AllRowsQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		predicate.setSlice_range(ThriftUtils.createSliceRange(columnFamily.getColumnSerializer(), startColumn, endColumn, reversed, count));
		return this;
	}

	@Override
	public AllRowsQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count) {
		predicate.setSlice_range(new SliceRange(startColumn, endColumn, reversed, count));
		return this;
	}
	
	@Override
	public AllRowsQuery<K, C> withColumnSlice(ColumnSlice<C> slice) {
		if (slice.getColumns() != null) {
        	predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(slice.getColumns())).setSlice_rangeIsSet(false);
		}
		else {
			predicate.setSlice_range(ThriftUtils.createSliceRange(columnFamily.getColumnSerializer(),
					slice.getStartColumn(), 
					slice.getEndColumn(), 
					slice.getReversed(), 
					slice.getLimit()));
		}
		return this;
	}
	
	@Override
	public AllRowsQuery<K, C> withColumnRange(ByteBufferRange range) {
		predicate.setSlice_range(
				new SliceRange()
					.setStart(range.getStart())
					.setFinish(range.getEnd())
					.setCount(range.getLimit())
					.setReversed(range.isReversed()));
		return this;
	}

	@Override
	public AllRowsQuery<K, C> setBlockSize(int blockSize) {
		range.setCount(blockSize);
		return this;
	}

	public int getBlockSize() {
		return range.getCount();
	}
	
	public void setLastRowKey(ByteBuffer rowKey) {
		// Determine the start token for the next page
		String token = partitioner.getToken(rowKey).toString();
		if (repeatLastToken) {
			BigInteger intToken = new BigInteger(token).subtract(new BigInteger("1"));
			range.setStart_token(intToken.toString());
		}
		else {
			range.setStart_token(token);
		}
	}
	
	@Override
	public AllRowsQuery<K, C> setRepeatLastToken(boolean repeatLastToken) {
		this.repeatLastToken = repeatLastToken;
		return this;
	}
	
	protected abstract List<org.apache.cassandra.thrift.KeySlice> getNextBlock() throws ConnectionException;
}

