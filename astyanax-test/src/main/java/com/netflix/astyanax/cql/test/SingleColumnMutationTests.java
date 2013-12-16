package com.netflix.astyanax.cql.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class SingleColumnMutationTests extends KeyspaceTests {

	public static ColumnFamily<Long, String> CF_SINGLE_COLUMN = ColumnFamily
			.newColumnFamily(
					"cfsinglecolmutation", 
					LongSerializer.get(),
					StringSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_SINGLE_COLUMN, null);
		CF_SINGLE_COLUMN.describe(keyspace);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_SINGLE_COLUMN);
	}

	@Test
	public void testSingleColumnMutation() throws Exception {

		keyspace.prepareColumnMutation(CF_SINGLE_COLUMN, 1L, "1").putValue("11", null).execute();
		keyspace.prepareColumnMutation(CF_SINGLE_COLUMN, 1L, "2").putValue("22", null).execute();
		keyspace.prepareColumnMutation(CF_SINGLE_COLUMN, 1L, "3").putValue("33", null).execute();
		
		ColumnList<String> result = keyspace.prepareQuery(CF_SINGLE_COLUMN).getRow(1L).execute().getResult();
		Assert.assertTrue(3 == result.size());
		
		Assert.assertEquals("11", result.getColumnByName("1").getStringValue());
		Assert.assertEquals("22", result.getColumnByName("2").getStringValue());
		Assert.assertEquals("33", result.getColumnByName("3").getStringValue());
		
		keyspace.prepareColumnMutation(CF_SINGLE_COLUMN, 1L, "2").putEmptyColumn(null).execute();
		keyspace.prepareColumnMutation(CF_SINGLE_COLUMN, 1L, "3").deleteColumn().execute();

		result = keyspace.prepareQuery(CF_SINGLE_COLUMN).getRow(1L).execute().getResult();
		Assert.assertTrue(2 == result.size());
		
		Assert.assertEquals("11", result.getColumnByName("1").getStringValue());
		Assert.assertNull(result.getColumnByName("2").getStringValue());
	}
}