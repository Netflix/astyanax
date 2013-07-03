package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class IntegerArraySerializer extends AbstractSerializer<Integer[]> {

	private static final IntegerArraySerializer instance = new IntegerArraySerializer();

	public static IntegerArraySerializer get() {
		return instance;
	}

	@Override
	public ByteBuffer toByteBuffer(Integer[] obj) {
		if (null == obj) {
			return null;
		}
		int size = obj.length;
		// Flags for null values in array, compacted
		// one byte represents 8 Integers, with each of its bit set to 1 for each non null Integer
		byte[] flags = new byte[size / 8 + 1];
		int[] data = new int[size];
		int dataIndex = 0;
		for (int i = 0; i < size; i++) {
			Integer val = obj[i];
			if (val != null) {
				int shift = i % 8;
				byte mask = (byte) (1 << shift);
				flags[i / 8] |= mask;
				data[dataIndex++] = val.intValue();
			}
		}
		data = Arrays.copyOf(data, dataIndex);

		ByteBuffer bbData = PrimitiveIntArraySerializer.get().toByteBuffer(data);
		int capacity = 4 + flags.length + bbData.remaining();
		ByteBuffer bb = ByteBuffer.allocate(capacity);
		bb.putInt(size).put(flags).put(bbData);
		bb.rewind();
		return bb;
	}

	@Override
	public Integer[] fromByteBuffer(ByteBuffer byteBuffer) {
		if (null == byteBuffer) {
			return null;
		}
		byteBuffer = byteBuffer.duplicate();
		int size = byteBuffer.getInt();
		byte[] flags = new byte[size / 8 + 1];
		byteBuffer.get(flags);
		int[] data = PrimitiveIntArraySerializer.get().fromByteBuffer(byteBuffer);

		Integer[] ints = new Integer[size];
		int dataIndex = 0;
		for (int i = 0; i < size; i++) {
			int shift = i % 8;
			byte mask = (byte) (1 << shift);
			boolean notNull = (flags[i / 8] & mask) != 0;
			if (notNull) {
				ints[i] = Integer.valueOf(data[dataIndex++]);
			}
		}
		return ints;
	}
}
