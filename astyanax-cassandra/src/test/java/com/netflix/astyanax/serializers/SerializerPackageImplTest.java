package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.Test;

public class SerializerPackageImplTest {
	@Test
	public void test() {
		SerializerPackageImpl serializers = new SerializerPackageImpl();
		try {
			serializers.setColumnType("CompositeType(UTF8Type, LongType)");
		} catch (UnknownComparatorException e) {
			e.printStackTrace();
			Assert.fail();
		}

		String input = "abc:123";
		ByteBuffer buffer = serializers.columnAsByteBuffer(input);
		String str = serializers.columnAsString(buffer);
		Assert.assertEquals(input, str);
	}

	@Test
	public void testSetCompositeKeyType() throws Exception
	{
		SerializerPackageImpl serializers = new SerializerPackageImpl();
		serializers.setKeyType( "CompositeType(UTF8Type, LongType)" );
		Assert.assertTrue(serializers.getKeySerializer() instanceof SpecificCompositeSerializer);
	}

	@Test
	public void testSetStandardKeyType() throws Exception
	{
		SerializerPackageImpl serializers = new SerializerPackageImpl();
		serializers.setKeyType( "LongType" );
		Assert.assertTrue(serializers.getKeySerializer() instanceof LongSerializer);
	}
}
