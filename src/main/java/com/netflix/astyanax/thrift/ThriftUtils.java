package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.Serializer;

public class ThriftUtils {
	public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
	public static final SliceRange RANGE_ALL = new SliceRange(EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);

	public static <C> SliceRange createSliceRange(Serializer<C> serializer, C startColumn, C endColumn, boolean reversed, int limit) {
		return new SliceRange(
				(startColumn == null) ? EMPTY_BYTE_BUFFER : serializer.toByteBuffer(startColumn),
				(endColumn == null) ? EMPTY_BYTE_BUFFER : serializer.toByteBuffer(endColumn),
				reversed, limit);
	
	}
}
