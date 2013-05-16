package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class PrimitiveIntArraySerializerTest {

	private final PrimitiveIntArraySerializer serializer = PrimitiveIntArraySerializer.get();

	@Test
	public void testNull() {
		int[] data = null;
		ByteBuffer bb = serializer.toByteBuffer(data);
		int[] actuals = serializer.fromByteBuffer(bb);
		Assert.assertNull(actuals);
	}

	@Test
	public void testEmpty() {
		int[] data = {};
		int[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		int[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	@Test
	public void testWithData() {
		int[] data = { 0, 1, 2, 5, -1, Integer.MIN_VALUE, Integer.MAX_VALUE };
		int[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		int[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	private void compareData(int[] expecteds, int[] actuals) {
		Assert.assertArrayEquals(expecteds, actuals);
	}

	private void compareSize(int[] data) {
		// Compare byte usage with ObjectSerializer.
		ByteBuffer optimizedBb = serializer.toByteBuffer(data);
		ByteBuffer objectBb = ObjectSerializer.get().toByteBuffer(data);
		System.out.printf("Optimized size: %s, Default size: %s%n", optimizedBb.remaining(), objectBb.remaining());
		Assert.assertTrue(optimizedBb.remaining() < objectBb.remaining());
	}
}
