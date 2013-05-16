package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DoubleArraySerializer extends AbstractSerializer<Double[]> {

	private static final DoubleArraySerializer instance = new DoubleArraySerializer();

	public static DoubleArraySerializer get() {
		return instance;
	}

	@Override
	public ByteBuffer toByteBuffer(Double[] obj) {
		if (null == obj) {
			return null;
		}
		int size = obj.length;
		// Flags for null values in array, compacted
		// one byte represents 8 Doubles, with each of its bit set to 1 for each non null Double
		byte[] flags = new byte[size / 8 + 1];
		double[] data = new double[size];
		int dataIndex = 0;
		for (int i = 0; i < size; i++) {
			Double val = obj[i];
			if (val != null) {
				int shift = i % 8;
				byte mask = (byte) (1 << shift);
				flags[i / 8] |= mask;
				data[dataIndex++] = val.doubleValue();
			}
		}
		data = Arrays.copyOf(data, dataIndex);

		ByteBuffer bbData = PrimitiveDoubleArraySerializer.get().toByteBuffer(data);
		int capacity = 4 + flags.length + bbData.remaining();
		ByteBuffer bb = ByteBuffer.allocate(capacity);
		bb.putInt(size).put(flags).put(bbData);
		bb.rewind();
		return bb;
	}

	@Override
	public Double[] fromByteBuffer(ByteBuffer byteBuffer) {
		if (null == byteBuffer) {
			return null;
		}
		byteBuffer = byteBuffer.duplicate();
		int size = byteBuffer.getInt();
		byte[] flags = new byte[size / 8 + 1];
		byteBuffer.get(flags);
		double[] data = PrimitiveDoubleArraySerializer.get().fromByteBuffer(byteBuffer);

		Double[] doubles = new Double[size];
		int dataIndex = 0;
		for (int i = 0; i < size; i++) {
			int shift = i % 8;
			byte mask = (byte) (1 << shift);
			boolean notNull = (flags[i / 8] & mask) != 0;
			if (notNull) {
				doubles[i] = Double.valueOf(data[dataIndex++]);
			}
		}
		return doubles;
	}
}
