package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class IntegerArraySerializerTest {

	private final IntegerArraySerializer serializer = IntegerArraySerializer.get();

	@Test
	public void testNull() {
		Integer[] data = null;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Integer[] actuals = serializer.fromByteBuffer(bb);
		Assert.assertNull(actuals);
	}

	@Test
	public void testEmpty() {
		Integer[] data = {};
		Integer[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Integer[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	@Test
	public void testWithData() {
		Integer[] data = { 0, 1, 2, 5, -1, Integer.MIN_VALUE, Integer.MAX_VALUE };
		Integer[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Integer[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	@Test
	public void testWithNullData() {
		for (int size = 0; size < 20; size++) {
			Integer[] data = new Integer[size];
			Integer[] expecteds = data;
			ByteBuffer bb = serializer.toByteBuffer(data);
			Integer[] actuals = serializer.fromByteBuffer(bb);
			compareData(expecteds, actuals);
			compareSize(data);
		}
	}

	@Test
	public void testWithMixedNullData() {
		Integer[] data = { null, 0, null, 1, null, 2, null, 5, null, -1, null, Integer.MIN_VALUE, Integer.MAX_VALUE,
				null };
		Integer[] expecteds = data;
		ByteBuffer bb = serializer.toByteBuffer(data);
		Integer[] actuals = serializer.fromByteBuffer(bb);
		compareData(expecteds, actuals);
		compareSize(data);
	}

	private void compareData(Integer[] expecteds, Integer[] actuals) {
		Assert.assertArrayEquals(expecteds, actuals);
	}

	private void compareSize(Integer[] data) {
		// Compare byte usage with ObjectSerializer.
		ByteBuffer optimizedBb = serializer.toByteBuffer(data);
		ByteBuffer objectBb = ObjectSerializer.get().toByteBuffer(data);
		System.out.printf("Optimized size: %s, Default size: %s%n", optimizedBb.remaining(), objectBb.remaining());
		Assert.assertTrue(optimizedBb.remaining() < objectBb.remaining());
	}
}
