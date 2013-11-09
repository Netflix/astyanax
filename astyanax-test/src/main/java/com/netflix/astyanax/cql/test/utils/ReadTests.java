package com.netflix.astyanax.cql.test.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import junit.framework.Assert;

import org.joda.time.DateTime;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.test.KeyspaceTests;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;


public class ReadTests extends KeyspaceTests {

	public static DateTime OriginalDate = new DateTime().withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0).withHourOfDay(0);
	public static byte[] TestBytes = new String("TestBytes").getBytes();
	public static UUID TestUUID = UUID.fromString("edeb3d70-15ce-11e3-8ffd-0800200c9a66");
	//public static int RowCount = 1;
	
	public static String[] columnNamesArr = {"firstname", "lastname", "address","age","ageShort", "ageLong","percentile", "married","single", "birthdate", "bytes", "uuid", "empty"};
	public static List<String> columnNames = new ArrayList<String>(Arrays.asList(columnNamesArr));
	
	public static ColumnFamily<String, String> CF_USER_INFO = ColumnFamily.newColumnFamily(
			"UserInfo", // Column Family Name
			StringSerializer.get(), // Key Serializer
			StringSerializer.get()); // Column Serializer

	public static void initReadTests() throws Exception {
		initContext();
		Collections.sort(columnNames); 
	}


    public void testAllColumnsForRow(ColumnList<String> resultColumns, int i) throws Exception {

    	Date date = OriginalDate.plusMinutes(i).toDate();

    	testColumnValue(resultColumns, "firstname", columnNames, "john_" + i);
    	testColumnValue(resultColumns, "lastname", columnNames, "smith_" + i);
    	testColumnValue(resultColumns, "address", columnNames, "john smith address " + i);
    	testColumnValue(resultColumns, "age", columnNames, 30 + i);
    	testColumnValue(resultColumns, "ageShort", columnNames, new Integer(30+i).shortValue());
    	testColumnValue(resultColumns, "ageLong", columnNames, new Integer(30+i).longValue());
    	testColumnValue(resultColumns, "percentile", columnNames, 30.1);
    	testColumnValue(resultColumns, "married", columnNames, true);
    	testColumnValue(resultColumns, "single", columnNames, false);
    	testColumnValue(resultColumns, "birthdate", columnNames, date);
    	testColumnValue(resultColumns, "bytes", columnNames, TestBytes);
    	testColumnValue(resultColumns, "uuid", columnNames, TestUUID);
    	testColumnValue(resultColumns, "empty", columnNames, null);
    	
    	/** TEST THE ITERATOR INTERFACE */
    	Iterator<Column<String>> iter = resultColumns.iterator();
    	while (iter.hasNext()) {
    		Column<String> col = iter.next();
    		Assert.assertNotNull(col.getName());
    	}
    }
    
    
    private <T> void testColumnValue(ColumnList<String> result, String columnName, List<String> columnNames, T expectedValue) {
    	
    	// by column name
    	Column<String> column = result.getColumnByName(columnName);
    	Assert.assertEquals(columnName, column.getName());
    	testColumnValue(column, expectedValue);
    	
//    	// by column index
//    	int index = columnNames.indexOf(columnName);
//    	column = result.getColumnByIndex(index);
//    	testColumnValue(column, expectedValue);
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
    
    public void populateRows(int numRows) throws Exception {

    	MutationBatch mb = keyspace.prepareMutationBatch();

    	for (int i=0; i<numRows; i++) {

    		Date date = OriginalDate.plusMinutes(i).toDate();
    		mb.withRow(CF_USER_INFO, "acct_" + i)
    		.putColumn("firstname", "john_" + i, null)
    		.putColumn("lastname", "smith_" + i, null)
    		.putColumn("address", "john smith address " + i, null)
    		.putColumn("age", 30+i, null)
    		.putColumn("ageShort", new Integer(30+i).shortValue(), null)
    		.putColumn("ageLong", new Integer(30+i).longValue(), null)
    		.putColumn("percentile", 30.1)
    		.putColumn("married", true)
    		.putColumn("single", false)
    		.putColumn("birthdate", date)
    		.putColumn("bytes", TestBytes)
    		.putColumn("uuid", TestUUID)
    		.putEmptyColumn("empty");

    		mb.execute();
    		mb.discardMutations();
    	}
    }
	
	
	public void deleteRows(int numRows) throws Exception {

        MutationBatch mb = keyspace.prepareMutationBatch();

        for (int i=0; i<numRows; i++) {
            mb.withRow(CF_USER_INFO, "acct_" + i).delete();
            mb.execute();
            mb.discardMutations();
        }
	}


	public Collection<String> getRandomColumns(int numColumns) {

		Random random = new Random();
		Set<String> hashSet = new HashSet<String>();

		while(hashSet.size() < numColumns) {
			int pick = random.nextInt(26);
			char ch = (char) ('a' + pick);
			hashSet.add(String.valueOf(ch));
		}
		return hashSet;
	}
    
}
