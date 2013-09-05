package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class MutiRowQueryTests extends ReadTests {

	private static String[] rowKeys; 
	private static int rowLookupCount = 2;

	static {
		rowKeys = new String[rowLookupCount];
    	for (int i=0; i<rowLookupCount; i++) {
    		rowKeys[i] = "acct_" + i;
    	}
	}
	
	@BeforeClass
	public static void init() throws Exception {
		initReadTests();
	}

	//@Test	
	public void testReadAllColumnsWithMultipleRowsKeys() throws Exception {
		
//		super.RowCount = 10;
//		super.populateRows();
//		Thread.sleep(1000);

		Rows<String, String> rows = keyspace.prepareQuery(CF_USER_INFO).getRowSlice(rowKeys).execute().getResult();
    	
    	Assert.assertFalse(rows.isEmpty());
    	int index = 0;
    	for (Row<String, String> row : rows) {
    		Assert.assertEquals("acct_" + index, row.getKey());
        	ColumnList<String> colList = row.getColumns();
        	super.testAllColumnsForRow(colList, index++);
    	}
    	
	}
	
	@Test	
	public void testReadSelectColumnsWithMultipleRowsKeys() throws Exception {
		
//		super.RowCount = 10;
//		super.populateRows();
//		Thread.sleep(1000);

    	Rows<String, String> rows = keyspace.prepareQuery(CF_USER_INFO)
    			.getRowSlice(rowKeys)
    			.withColumnSlice(columnNames).execute().getResult();
    	
    	Assert.assertFalse(rows.isEmpty());
    	int index = 0;
    	for (Row<String, String> row : rows) {
    		Assert.assertEquals("acct_" + index, row.getKey());
        	ColumnList<String> colList = row.getColumns();
        	super.testAllColumnsForRow(colList, index++);
    	}
    	
    	super.RowCount = 100;
    	super.deleteRows();
    	Thread.sleep(1000);
    	
    	rows = keyspace.prepareQuery(CF_USER_INFO)
    			.getRowSlice(rowKeys)
    			.withColumnSlice(columnNames).execute().getResult();
    	
    	Assert.assertTrue(rows.isEmpty());
	}
	
	
	
}
