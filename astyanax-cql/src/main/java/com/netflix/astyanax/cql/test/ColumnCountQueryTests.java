package com.netflix.astyanax.cql.test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

public class ColumnCountQueryTests extends ReadTests {

	@BeforeClass
	public static void init() throws Exception {
		initContext();
	}
	
	@Test
	public void test() throws Exception {
		
//		testColumnCountSingleRowAndAllColumns(); 
//		testColumnCountSingleRowAndColumnSet(); 
//		testColumnCountSingleRowAndColumnRange();
//		
//		testColumnCountMultipleRowKeysAndAllColumns(); 
//		testColumnCountMultipleRowKeysAndColumnSet();
		testColumnCountMultipleRowKeysAndColumnRange(); 
//		
//		testColumnCountRowRangeAndAllColumns(); 
//		testColumnCountRowRangeAndColumnSet(); 
//		testColumnCountRowRangeAndColumnRange(); 

		
	}
	
	public void testColumnCountSingleRowAndAllColumns() throws Exception {

		char ch = 'A';
		while (ch <= 'Z') {
			String rowKey = String.valueOf(ch);

			Integer columnCount = keyspace
					.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getKey(rowKey)
					.getCount()
					.execute().getResult();

			int expected = 26;
			Assert.assertTrue("expected: " + expected + " colCount: " + columnCount, expected == columnCount);
			ch++;
		}
	}
	
	public void testColumnCountSingleRowAndColumnSet() throws Exception {

		Random random = new Random();
		
		char ch = 'A';
		while (ch <= 'Z') {
			
			String rowKey = String.valueOf(ch);
			int numColumns = random.nextInt(26) + 1; // avoid 0
			
			Integer columnCount = keyspace
					.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getKey(rowKey)
					.withColumnSlice(getRandomColumns(numColumns))
					.getCount()
					.execute().getResult();

			int expected = numColumns;
			Assert.assertTrue("expected: " + expected + " colCount: " + columnCount, expected == columnCount);
			ch++;
		}
	}
	
	

	public void testColumnCountSingleRowAndColumnRange() throws Exception {
		
		Random random = new Random();
		
		char ch = 'A';
		while (ch <= 'Z') {

			String rowKey = String.valueOf(ch);
			
			// Get a random start column
			int rand = random.nextInt(26);
			char randCH = (char) ('a' + rand);
			String startCol = String.valueOf(randCH);
			
			Integer columnCount = keyspace
					.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getKey(rowKey)
					.withColumnRange(startCol, "z", false, -1)
					.getCount()
					.execute().getResult();

			int charOffset = startCol.charAt(0) - 'a' + 1;
			int expected = 26 - charOffset + 1;
			
			/**
			 * e.g  if start col = 'b' 
			 *      then range = 'b' -> 'z' both inclusive 
			 *      then colCount = 25 
			 *      where
			 *      'a' - 'b' + 1 = 2 which is offset for 'b'
			 *      25 = 26 ('z') - 2('b') + 1 
			 */
	 		
			Assert.assertTrue("expected: " + expected + " colCount: " + columnCount, expected == columnCount);
			ch++;
		}
	}
	
	public void testColumnCountMultipleRowKeysAndAllColumns() throws Exception {

		Collection<String> rowKeys = getRandomRowKeys();
		
		Map<String, Integer> columnCountsPerRowKey = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getRowSlice(rowKeys)
				.getColumnCounts()
				.execute().getResult();

		Map<String, Integer> expected = new HashMap<String, Integer>();
		for (String key : rowKeys) {
			expected.put(key, 26);
		}
		Assert.assertEquals("expected: " + expected + " colCount: " + columnCountsPerRowKey, expected, columnCountsPerRowKey);
	}

	public void testColumnCountMultipleRowKeysAndColumnSet() throws Exception {

		Collection<String> rowKeys = getRandomRowKeys();
		Collection<String> columns = getRandomColumns(new Random().nextInt(26) + 1);
		
		
		Map<String, Integer> columnCountsPerRowKey = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getRowSlice(rowKeys)
				.withColumnSlice(columns)
				.getColumnCounts()
				.execute().getResult();

		Map<String, Integer> expected = new HashMap<String, Integer>();
		for (String key : rowKeys) {
			expected.put(key, columns.size());
		}
		Assert.assertEquals("expected: " + expected + " colCount: " + columnCountsPerRowKey, expected, columnCountsPerRowKey);
	}
	
	public void testColumnCountMultipleRowKeysAndColumnRange() throws Exception {

		Collection<String> rowKeys = getRandomRowKeys();

		// Get a random start column
		int rand = new Random().nextInt(26);
		char randCH = (char) ('a' + rand);
		String startCol = String.valueOf(randCH);
		
		Map<String, Integer> columnCountsPerRowKey = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getRowSlice(rowKeys)
				.withColumnRange(startCol, "z", false, -1)
				.getColumnCounts()
				.execute().getResult();

		int charOffset = startCol.charAt(0) - 'a' + 1;
		int expectedColCount = 26 - charOffset + 1;

		Map<String, Integer> expected = new HashMap<String, Integer>();
		for (String key : rowKeys) {
			expected.put(key, expectedColCount);
		}
		Assert.assertEquals("expected: " + expected + " colCount: " + columnCountsPerRowKey, expected, columnCountsPerRowKey);
	}



	private Collection<String> getRandomRowKeys() {

		Random random = new Random();
		
		int numRowKeys = random.nextInt(26) + 1;
		
		Set<String> hashSet = new HashSet<String>();

		while(hashSet.size() < numRowKeys) {
			int pick = random.nextInt(26);
			char ch = (char) ('A' + pick);
			hashSet.add(String.valueOf(ch));
		}
		return hashSet;
	}
}
