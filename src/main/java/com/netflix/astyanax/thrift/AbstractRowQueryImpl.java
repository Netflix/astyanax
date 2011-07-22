package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.RowQuery;

public abstract class AbstractRowQueryImpl<K, C> implements  RowQuery<K, C> {
	
	protected SlicePredicate predicate = new SlicePredicate().setSlice_range(ThriftUtils.RANGE_ALL);
	private Serializer<C> serializer;
	
	public AbstractRowQueryImpl(Serializer<C> serializer) {
		this.serializer = serializer;
	}
	
    @Override
	public RowQuery<K, C> withColumnSlice(C... columns) {
    	predicate.setColumn_names(serializer.toBytesList(Arrays.asList(columns))).setSlice_rangeIsSet(false);
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		predicate.setSlice_range(ThriftUtils.createSliceRange(serializer, startColumn, endColumn, reversed, count));
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnSlice(ColumnSlice<C> slice) {
		if (slice.getColumns() != null) {
        	predicate.setColumn_names(serializer.toBytesList(slice.getColumns())).setSlice_rangeIsSet(false);
		}
		else {
			predicate.setSlice_range(ThriftUtils.createSliceRange(serializer,
					slice.getStartColumn(), 
					slice.getEndColumn(), 
					slice.getReversed(), 
					slice.getLimit()));
		}
		return this;
	}

	@Override
	public RowQuery<K, C> withColumnRange(ByteBuffer startColumn,
			ByteBuffer endColumn, boolean reversed, int count) {
		predicate.setSlice_range(new SliceRange(startColumn, endColumn, reversed, count));
		return this;
	}
	
	@Override
	public RowQuery<K, C> withColumnRange(ByteBufferRange range) {
		predicate.setSlice_range(
				new SliceRange()
					.setStart(range.getStart())
					.setFinish(range.getEnd())
					.setCount(range.getLimit())
					.setReversed(range.isReversed()));
		return this;
	}
}
