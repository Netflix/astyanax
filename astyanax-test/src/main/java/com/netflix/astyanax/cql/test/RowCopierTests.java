package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class RowCopierTests extends KeyspaceTests {

	private static final ColumnFamily<Integer, String> CF_ROW_COPY = 
			new ColumnFamily<Integer, String>("testrowcopy", IntegerSerializer.get(), StringSerializer.get(), IntegerSerializer.get());
	private static final ColumnFamily<Integer, String> CF_ROW_COPY2 = 
			new ColumnFamily<Integer, String>("testrowcopy2", IntegerSerializer.get(), StringSerializer.get(), IntegerSerializer.get());

	@BeforeClass
	public static void init() throws Exception {

		initContext();
		
		keyspace.createColumnFamily(CF_ROW_COPY, null);
		keyspace.createColumnFamily(CF_ROW_COPY2, null);
		
		CF_ROW_COPY.describe(keyspace);
		CF_ROW_COPY2.describe(keyspace);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_ROW_COPY);
		keyspace.dropColumnFamily(CF_ROW_COPY2);
	}
	
	@Test
	public void runRowCopyTest() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(CF_ROW_COPY, 10).putColumn("c1", 1).putColumn("c2", 2);
		m.execute();
		
		ColumnList<String> result = keyspace.prepareQuery(CF_ROW_COPY).getRow(10).execute().getResult();
		
		Column<String> column = result.getColumnByIndex(0);
		Assert.assertEquals("c1", column.getName());
		Assert.assertEquals(1, column.getIntegerValue());
		column = result.getColumnByIndex(1);
		Assert.assertEquals("c2", column.getName());
		Assert.assertEquals(2, column.getIntegerValue());
		
		keyspace.prepareQuery(CF_ROW_COPY).getRow(10).copyTo(CF_ROW_COPY2, 11).execute();
		
		ColumnList<String> result2 = keyspace.prepareQuery(CF_ROW_COPY2).getRow(11).execute().getResult();
		
		column = result2.getColumnByIndex(0);
		Assert.assertEquals("c1", column.getName());
		Assert.assertEquals(1, column.getIntegerValue());
		column = result2.getColumnByIndex(1);
		Assert.assertEquals("c2", column.getName());
		Assert.assertEquals(2, column.getIntegerValue());
	}
}
