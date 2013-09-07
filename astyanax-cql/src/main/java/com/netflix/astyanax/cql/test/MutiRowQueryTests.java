package com.netflix.astyanax.cql.test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.serializers.StringSerializer;

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
	public void testSort() throws Exception {
	
		
		//SortedMap<String, String> md5Mapping = new TreeMap<String, String>();
		SortedMap<BigInteger, String> reverseMapping = new TreeMap<BigInteger, String>();
		
		
		for (char ch = 'A'; ch <='Z'; ch++) {
			
			String s = String.valueOf(ch);
			ByteBuffer bb = StringSerializer.get().toByteBuffer(s);
			String hash = Murmur3Partitioner.get().getTokenForKey(bb);
			
			
			reverseMapping.put(new BigInteger(hash), s);
		}
		
		System.out.println("Reverse Map " + reverseMapping);
		
		int count = 1; 
		Iterator<BigInteger> iter = reverseMapping.keySet().iterator();
		
		String startToken = null;
		String endToken = null;
		List<String> list = new ArrayList<String>();
		while (count <= 24) {
			
			BigInteger token = iter.next();

			list.add(reverseMapping.get(token));
			
			if ((count-1)%3 == 0) {
				startToken = String.valueOf(token);
			}
			if (count%3 == 0) {
				endToken = String.valueOf(token);
				StringBuilder sb = new StringBuilder("tokenRanges.add(new TestTokenRange(\"" + startToken + "\", \"" + endToken + "\",");
				Iterator<String> iter1 = list.iterator();
				while (iter1.hasNext()) {
					sb.append("\"" + iter1.next() + "\"");
					if (iter1.hasNext()) {
						sb.append(", ");
					}
				}
				sb.append("));");
				System.out.println(sb.toString());
				list = new ArrayList<String>();
			}
			
			
			count++;
		}
		
//		for (String key : reverseMapping.keySet()) {
//			String v1 = reverseMapping.get(key);
//			String v2 = reverseMapping2.get(key);
//			if (!v1.equals(v2)) {
//				System.out.println("Mismatch: " + key + " " + v1 + " " + v2);
//			}
//		}
	}

	//@Test
	public void testRowRangesForAllColumns() throws Exception {

		List<String> expectedColumns = new ArrayList<String>();
		for (char ch = 'a'; ch <= 'z'; ch++) {
			expectedColumns.add(String.valueOf(ch));
		}
		
		for (TestTokenRange testRange : getTestTokenRanges()) {
		
			Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.execute().getResult();

			Assert.assertFalse(rows.isEmpty());
			
			List<String> list = new ArrayList<String>();
			for (Row<String, String> row : rows) {
				String key = row.getKey();
				list.add(key);
				
				ColumnList<String> columns = row.getColumns();
				testRangeColumnsForRow(columns, expectedColumns);
			}
			
			Assert.assertEquals(testRange.expectedRowKeys, list);
		}
	}
	
	@Test
	public void testRowRangesForColumnSet() throws Exception {

		Random random = new Random();
		
		//for (TestTokenRange testRange : getTestTokenRanges()) {
		
		TestTokenRange testRange = getTestTokenRanges().get(0);
		
			Collection<String> randomColumns = getRandomColumns(random.nextInt(26) + 1);
			
			Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.withColumnSlice(randomColumns)
					.execute().getResult();

			Assert.assertFalse(rows.isEmpty());
			
			List<String> list = new ArrayList<String>();
			for (Row<String, String> row : rows) {
				String key = row.getKey();
				list.add(key);
				
				ColumnList<String> columns = row.getColumns();
				testRangeColumnsForRow(columns, new ArrayList<String>(randomColumns));
			}
			
			Assert.assertEquals(testRange.expectedRowKeys, list);
		//}
	}
	
	private void testRangeColumnsForRow(ColumnList<String> columns, List<String> expected) {
		
		Iterator<Column<String>> iter1 = columns.iterator();
		Iterator<String> iter2 = expected.iterator();
		while (iter2.hasNext()) {
			
			Column<String> column = iter1.next();
			String expectedName = iter2.next();
			
			Assert.assertEquals(expectedName, column.getName());
			int expectedValue = expectedName.charAt(0) - 'a' + 1;
			Assert.assertEquals(expectedValue, column.getIntegerValue());
		}
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
	
	//@Test	
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
