package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class DoubleArraySerializerTest {

	private final DoubleArraySerializer serializer = DoubleArraySerializer.get();

	@Test
	public void testNull() {
		Double[] data = null;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Double[] actuals = serializer.fromByteBuffer(bb);
		Assert.assertNull(actuals);
	}

	@Test
	public void testEmpty() {
		Double[] data = {};
		Double[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Double[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	@Test
	public void testWithData() {
		Double[] data = { 0.0, 1.0, 2.0, 0.5, 2.5, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN,
				Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_NORMAL };
		Double[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Double[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	@Test
	public void testWithNullData() {
		for (int size = 0; size < 20; size++) {
			Double[] data = new Double[size];
			Double[] expecteds = data;
			ByteBuffer bb = serializer.toByteBuffer(data);
			Double[] actuals = serializer.fromByteBuffer(bb);
			compareData(expecteds, actuals);
			compareSize(data);
		}
	}

	@Test
	public void testWithMixedNullData() {
		Double[] data = { null, 0.0, null, 1.0, null, 2.0, null, 0.5, null, 2.5, null, Double.MIN_VALUE,
				Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_NORMAL,
				null };
		Double[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Double[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	private void compareData(Double[] expecteds, Double[] actuals) {
		Assert.assertArrayEquals(expecteds, actuals);
	}

	private void compareSize(Double[] data) {
		// Compare byte usage with ObjectSerializer.
		ByteBuffer optimizedBb = serializer.toByteBuffer(data);
		ByteBuffer objectBb = ObjectSerializer.get().toByteBuffer(data);
		System.out.printf("Optimized size: %s, Default size: %s%n", optimizedBb.remaining(), objectBb.remaining());
		Assert.assertTrue(optimizedBb.remaining() < objectBb.remaining());
	}
}
