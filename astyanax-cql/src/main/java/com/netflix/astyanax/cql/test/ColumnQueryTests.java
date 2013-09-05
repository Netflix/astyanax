package com.netflix.astyanax.cql.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.BytesArraySerializer;

public class ColumnQueryTests extends ReadTests {

	@BeforeClass
	public static void init() throws Exception {
		initReadTests();
	}
	
	public void testRowQueryEntireRow() throws Exception {
		
    	populateRows();
    	Thread.sleep(1000);

    	testRowQueryWithDifferentDataTypes();
    	
    	deleteRows();
    	Thread.sleep(1000);
    	testMissingRowQuery();
    }


	public void testSingleRowSingleColumn() throws Exception {
		
    	populateRows();
    	Thread.sleep(1000);

    	testSingleRowColumnQuery();
    	
    	deleteRows();
    	Thread.sleep(1000);
    	testMissingRowColumnQuery();
    }

	public void testSingleRowColumnSliceQueryTest() throws Exception {
		
    	populateRows();
    	Thread.sleep(1000);

    	testSingleRowColumnSliceQueryWithCollection();
    	testSingleRowColumnSliceQueryVarArgs();
    	
    	deleteRows();
    	Thread.sleep(1000);
    	testMissingRowColumnSliceQueryWithCollection();
    	testMissingRowColumnSliceQueryVarArgs();
    }
	
	@Test
	public void testSingleRowColumnCountQueryTest() throws Exception {
    	populateRows();
    	Thread.sleep(1000);

    	testSingleRowColumnCountQuery(columnNames.size());
    	
//    	deleteRows();
//    	Thread.sleep(1000);
//    	testSingleRowColumnCountQuery(0);
	}


	private void testSingleRowColumnCountQuery(int expected) throws Exception {
        for (int i=0; i<RowCount; i++) {
        	int count = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i).getCount().execute().getResult().intValue();
        	Assert.assertEquals(expected, count);
        }
	}

	private void testMissingRowColumnSliceQueryVarArgs() throws Exception {
        for (int i=0; i<RowCount; i++) {
        	ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i)
        			.withColumnSlice("firstname", "lastname", "address","age","ageShort", "ageLong","percentile", "married","single", "birthdate", "bytes", "uuid", "empty").execute().getResult();
        	Assert.assertTrue(response.isEmpty());
        }
	}


	private void testMissingRowColumnSliceQueryWithCollection() throws Exception {
        for (int i=0; i<RowCount; i++) {
        	ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i).withColumnSlice(columnNames).execute().getResult();
        	Assert.assertTrue(response.isEmpty());
        }
	}

	private void testSingleRowColumnSliceQueryWithCollection() throws Exception {
    	
    	/**
    	 * READ BY COLUMN SLICE COLLECTION
    	 */
        for (int i=0; i<RowCount; i++) {

        	Date date = OriginalDate.plusMinutes(i).toDate();

        	ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i).withColumnSlice(columnNames).execute().getResult();
        	
        	testColumnValue(response, "firstname", columnNames, "john_" + i);
        	testColumnValue(response, "lastname", columnNames, "smith_" + i);
        	testColumnValue(response, "address", columnNames, "john smith address " + i);
        	testColumnValue(response, "age", columnNames, 30 + i);
        	testColumnValue(response, "ageShort", columnNames, new Integer(30+i).shortValue());
        	testColumnValue(response, "ageLong", columnNames, new Integer(30+i).longValue());
        	testColumnValue(response, "percentile", columnNames, 30.1);
        	testColumnValue(response, "married", columnNames, true);
        	testColumnValue(response, "single", columnNames, false);
        	testColumnValue(response, "birthdate", columnNames, date);
        	testColumnValue(response, "bytes", columnNames, TestBytes);
        	testColumnValue(response, "uuid", columnNames, TestUUID);
        	testColumnValue(response, "empty", columnNames, null);

        }
    }

    private void testSingleRowColumnSliceQueryVarArgs() throws Exception {
    	
    	/**
    	 * READ BY COLUMN SLICE COLLECTION
    	 */
        for (int i=0; i<RowCount; i++) {

        	Date date = OriginalDate.plusMinutes(i).toDate();

        	ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i)
        			.withColumnSlice("firstname", "lastname", "address","age","ageShort", "ageLong","percentile", "married","single", "birthdate", "bytes", "uuid", "empty").execute().getResult();
        	
        	testColumnValue(response, "firstname", columnNames, "john_" + i);
        	testColumnValue(response, "lastname", columnNames, "smith_" + i);
        	testColumnValue(response, "address", columnNames, "john smith address " + i);
        	testColumnValue(response, "age", columnNames, 30 + i);
        	testColumnValue(response, "ageShort", columnNames, new Integer(30+i).shortValue());
        	testColumnValue(response, "ageLong", columnNames, new Integer(30+i).longValue());
        	testColumnValue(response, "percentile", columnNames, 30.1);
        	testColumnValue(response, "married", columnNames, true);
        	testColumnValue(response, "single", columnNames, false);
        	testColumnValue(response, "birthdate", columnNames, date);
        	testColumnValue(response, "bytes", columnNames, TestBytes);
        	testColumnValue(response, "uuid", columnNames, TestUUID);
        	testColumnValue(response, "empty", columnNames, null);

        }
    }
	
    private void testSingleRowColumnQuery() throws Exception {
    	
        /** NOW READ 1000 ROWS BACK */
    	
    	/**
    	 * READ BY COLUMN NAME
    	 */
        for (int i=0; i<RowCount; i++) {

        	Date date = OriginalDate.plusMinutes(i).toDate();

        	Column<String> column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("firstname").execute().getResult();
        	testColumnValue(column, "john_" + i);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("lastname").execute().getResult();
        	testColumnValue(column, "smith_" + i);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("address").execute().getResult();
        	testColumnValue(column, "john smith address " + i);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("age").execute().getResult();
        	testColumnValue(column, 30 + i);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("ageShort").execute().getResult();
        	testColumnValue(column, new Integer(30+i).shortValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("ageLong").execute().getResult();
        	testColumnValue(column, new Integer(30+i).longValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("percentile").execute().getResult();
        	testColumnValue(column, 30.1);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("married").execute().getResult();
        	testColumnValue(column, true);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("single").execute().getResult();
        	testColumnValue(column, false);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("birthdate").execute().getResult();
        	testColumnValue(column, date);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("bytes").execute().getResult();
        	testColumnValue(column, TestBytes);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("uuid").execute().getResult();
        	testColumnValue(column, TestUUID);
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("empty").execute().getResult();
        	testColumnValue(column, null);
        }
    }
    
    private void testMissingRowColumnQuery() throws Exception {
    	
        /** NOW READ 1000 ROWS BACK */
    	
    	/**
    	 * READ BY COLUMN NAME
    	 */
        for (int i=0; i<RowCount; i++) {

        	Column<String> column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("firstname").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("lastname").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("address").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("age").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("ageShort").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("ageLong").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("percentile").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("married").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("single").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("birthdate").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("bytes").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("uuid").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        	column = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i) .getColumn("empty").execute().getResult();
        	Assert.assertFalse(column.hasValue());
        }
    }
    

	
    private void testRowQueryWithDifferentDataTypes() throws Exception {
    	
    	String[] arr = {"firstname", "lastname", "address","age","ageShort", "ageLong","percentile", "married","single", "birthdate", "bytes", "uuid", "empty"};
    	List<String> columnNames = new ArrayList<String>(Arrays.asList(arr));
    	Collections.sort(columnNames);
    	
        /** NOW READ 1000 ROWS BACK */
    	
    	/**
    	 * READ BY COLUMN NAME
    	 */
        for (int i=0; i<RowCount; i++) {

        	ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i).execute().getResult();

        	Assert.assertFalse(response.isEmpty());
        	
        	List<String> columnNameList = new ArrayList<String>(response.getColumnNames());
        	Collections.sort(columnNameList);
        	
        	Assert.assertEquals(columnNames, columnNameList);
        	Date date = OriginalDate.plusMinutes(i).toDate();

        	testColumnValue(response, "firstname", columnNames, "john_" + i);
        	testColumnValue(response, "lastname", columnNames, "smith_" + i);
        	testColumnValue(response, "address", columnNames, "john smith address " + i);
        	testColumnValue(response, "age", columnNames, 30 + i);
        	testColumnValue(response, "ageShort", columnNames, new Integer(30+i).shortValue());
        	testColumnValue(response, "ageLong", columnNames, new Integer(30+i).longValue());
        	testColumnValue(response, "percentile", columnNames, 30.1);
        	testColumnValue(response, "married", columnNames, true);
        	testColumnValue(response, "single", columnNames, false);
        	testColumnValue(response, "birthdate", columnNames, date);
        	testColumnValue(response, "bytes", columnNames, TestBytes);
        	testColumnValue(response, "uuid", columnNames, TestUUID);
        	testColumnValue(response, "empty", columnNames, null);
        	
        	/** TEST THE ITERATOR INTERFACE */
        	Iterator<Column<String>> iter = response.iterator();
        	Iterator<String> columnNameIter = columnNames.iterator();
        	while (iter.hasNext()) {
        		Column<String> col = iter.next();
        		String columnName = columnNameIter.next();
        		Assert.assertEquals(columnName, col.getName());
        	}
        }
    }


    
    private void testMissingRowQuery() throws Exception {
    	for (int i=0; i<RowCount; i++) {
    		ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct_" + i).execute().getResult();
    		Assert.assertTrue(response.isEmpty());
    	}
    }
    
    private <T> void testColumnValue(ColumnList<String> response, String columnName, List<String> columnNames, T value) {
    	
    	// by column name
    	Column<String> column = response.getColumnByName(columnName);
    	Assert.assertEquals(columnName, column.getName());
    	testColumnValue(column, value);
    	
    	// by column index
    	int index = columnNames.indexOf(columnName);
    	column = response.getColumnByIndex(index);
    	testColumnValue(column, value);
    }
    
    private <T> void testColumnValue(Column<String> column, T value) {

    	// Check the column name
    	// check if value exists
    	if (value != null) {
    		Assert.assertTrue(column.hasValue());
    		if (value instanceof String) {
        		Assert.assertEquals(value, column.getStringValue());
    		} else if (value instanceof Integer) {
        		Assert.assertEquals(value, column.getIntegerValue());
    		} else if (value instanceof Short) {
        		Assert.assertEquals(value, column.getShortValue());
    		} else if (value instanceof Long) {
        		Assert.assertEquals(value, column.getLongValue());
    		} else if (value instanceof Double) {
        		Assert.assertEquals(value, column.getDoubleValue());
    		} else if (value instanceof Boolean) {
        		Assert.assertEquals(value, column.getBooleanValue());
    		} else if (value instanceof Date) {
        		Assert.assertEquals(value, column.getDateValue());
    		} else if (value instanceof byte[]) {
    			ByteBuffer bbuf = column.getByteBufferValue();
    			String result = new String(BytesArraySerializer.get().fromByteBuffer(bbuf));
    			Assert.assertEquals(new String((byte[])value), result);
    		} else if (value instanceof UUID) {
        		Assert.assertEquals(value, column.getUUIDValue());
    		} else {
    			Assert.fail("Value not recognized for column: " + column.getName()); 
    		}
    	} else {
    		// check that value does not exist
    		Assert.assertFalse(column.hasValue());
    	}
    }
    

}
