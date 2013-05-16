package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class PrimitiveDoubleArraySerializerTest {

	private final PrimitiveDoubleArraySerializer serializer = PrimitiveDoubleArraySerializer.get();

	@Test
	public void testNull() {
		double[] data = null;
		ByteBuffer bb = serializer.toByteBuffer(data);
		double[] actuals = serializer.fromByteBuffer(bb);
		Assert.assertNull(actuals);
	}

	@Test
	public void testEmpty() {
		double[] data = {};
		double[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		double[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	@Test
	public void testWithData() {
		double[] data = { 0.0, 1.0, 2.0, 0.5, 2.5, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN,
				Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_NORMAL };
		double[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		double[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	private void compareData(double[] expecteds, double[] actuals) {
		Assert.assertArrayEquals(expecteds, actuals, 0.0);
	}

	private void compareSize(double[] data) {
		// Compare byte usage with ObjectSerializer.
		ByteBuffer optimizedBb = serializer.toByteBuffer(data);
		ByteBuffer objectBb = ObjectSerializer.get().toByteBuffer(data);
		System.out.printf("Optimized size: %s, Default size: %s%n", optimizedBb.remaining(), objectBb.remaining());
		Assert.assertTrue(optimizedBb.remaining() < objectBb.remaining());
	}
}
