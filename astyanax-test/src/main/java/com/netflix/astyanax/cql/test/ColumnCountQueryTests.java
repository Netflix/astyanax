package com.netflix.astyanax.cql.test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.test.utils.ReadTests;
import com.netflix.astyanax.cql.test.utils.TestUtils;
import com.netflix.astyanax.cql.test.utils.TestUtils.TestTokenRange;
import com.netflix.astyanax.model.ColumnFamily;

public class ColumnCountQueryTests extends ReadTests {

	private static ColumnFamily<String, String> CF_COLUMN_RANGE_TEST = TestUtils.CF_COLUMN_RANGE_TEST;

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_COLUMN_RANGE_TEST, null);
		CF_COLUMN_RANGE_TEST.describe(keyspace);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_COLUMN_RANGE_TEST);
	}

	@Test
	public void runAllTests() throws Exception {
		
		CF_COLUMN_RANGE_TEST.describe(keyspace);
		
		boolean rowDeleted = false; 
		
		/** INSERT ROWS FOR COLUMN COUNT READ TESTS */
		populateRowsForColumnRange();
		Thread.sleep(1000);
		
		/** PERFORM READS AND CHECK FOR COLUMN COUNTS */
		testColumnCountSingleRowAndAllColumns(rowDeleted); 
		testColumnCountSingleRowAndColumnSet(rowDeleted); 
		testColumnCountSingleRowAndColumnRange(rowDeleted);
		
		testColumnCountMultipleRowKeysAndAllColumns(rowDeleted); 
		testColumnCountMultipleRowKeysAndColumnSet(rowDeleted);
		testColumnCountMultipleRowKeysAndColumnRange(rowDeleted);
		
		testColumnCountRowRangeAndAllColumns(rowDeleted); 
		testColumnCountRowRangeAndColumnSet(rowDeleted); 
		testColumnCountRowRangeAndColumnRange(rowDeleted); 

		/** DELETE ROWS */
		deleteRowsForColumnRange(); 
		Thread.sleep(1000);
		rowDeleted = true; 
		
		/** PERFORM READS AND CHECK FOR COLUMN COUNTS  = 0 */
		testColumnCountSingleRowAndAllColumns(rowDeleted); 
		testColumnCountSingleRowAndColumnSet(rowDeleted); 
		testColumnCountSingleRowAndColumnRange(rowDeleted);
		
		testColumnCountMultipleRowKeysAndAllColumns(rowDeleted); 
		testColumnCountMultipleRowKeysAndColumnSet(rowDeleted);
		testColumnCountMultipleRowKeysAndColumnRange(rowDeleted); 

		testColumnCountRowRangeAndAllColumns(rowDeleted); 
		testColumnCountRowRangeAndColumnSet(rowDeleted); 
		testColumnCountRowRangeAndColumnRange(rowDeleted); 
	}
	
	private void testColumnCountSingleRowAndAllColumns(boolean rowDeleted) throws Exception {

		char ch = 'A';
		while (ch <= 'Z') {
			String rowKey = String.valueOf(ch);

			Integer columnCount = keyspace
					.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getKey(rowKey)
					.getCount()
					.execute().getResult();

			int expected = rowDeleted ? 0 : 26;
			Assert.assertTrue("expected: " + expected + " colCount: " + columnCount, expected == columnCount);
			ch++;
		}
	}
	
	private void testColumnCountSingleRowAndColumnSet(boolean rowDeleted) throws Exception {

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

			int expected = rowDeleted ? 0 : numColumns;
			Assert.assertTrue("expected: " + expected + " colCount: " + columnCount, expected == columnCount);
			ch++;
		}
	}
	
	private void testColumnCountSingleRowAndColumnRange(boolean rowDeleted) throws Exception {
		
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
			int expected = rowDeleted ? 0 : 26 - charOffset + 1;
			
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
	
	private void testColumnCountMultipleRowKeysAndAllColumns(boolean rowDeleted) throws Exception {

		Collection<String> rowKeys = getRandomRowKeys();
		
		Map<String, Integer> columnCountsPerRowKey = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getRowSlice(rowKeys)
				.getColumnCounts()
				.execute().getResult();

		Map<String, Integer> expected = new HashMap<String, Integer>();
		if (!rowDeleted) {
			for (String key : rowKeys) {
				expected.put(key, 26);
			}
		}
		Assert.assertEquals("expected: " + expected + " colCount: " + columnCountsPerRowKey, expected, columnCountsPerRowKey);
	}

	private void testColumnCountMultipleRowKeysAndColumnSet(boolean rowDeleted) throws Exception {

		Collection<String> rowKeys = getRandomRowKeys();
		Collection<String> columns = getRandomColumns(new Random().nextInt(26) + 1);
		
		
		Map<String, Integer> columnCountsPerRowKey = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getRowSlice(rowKeys)
				.withColumnSlice(columns)
				.getColumnCounts()
				.execute().getResult();

		Map<String, Integer> expected = new HashMap<String, Integer>();
		if (!rowDeleted) {
			for (String key : rowKeys) {
				expected.put(key, columns.size());
			}
		}
		Assert.assertEquals("expected: " + expected + " colCount: " + columnCountsPerRowKey, expected, columnCountsPerRowKey);
	}
	
	private void testColumnCountMultipleRowKeysAndColumnRange(boolean rowDeleted) throws Exception {

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
		if (!rowDeleted) {
			for (String key : rowKeys) {
				expected.put(key, expectedColCount);
			}
		}
		Assert.assertEquals("expected: " + expected + " colCount: " + columnCountsPerRowKey, expected, columnCountsPerRowKey);
	}
	
	private void testColumnCountRowRangeAndAllColumns(boolean rowDeleted) throws Exception {

		List<TestTokenRange> testRanges = TestUtils.getTestTokenRanges();
		
		Map<String, Integer> expectedRowCounts = rowDeleted ? new HashMap<String, Integer>() : getExpectedRowCountsForTokenRanges(testRanges, 26);
		Map<String, Integer> resultRowCounts = new HashMap<String, Integer>();
		
		for (TestTokenRange testRange : testRanges) {
			
			Map<String, Integer> rowCounts = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.getColumnCounts()
					.execute().getResult();

			resultRowCounts.putAll(rowCounts);
		}
		
		Assert.assertEquals(expectedRowCounts, resultRowCounts);
	}
	
	private void testColumnCountRowRangeAndColumnSet(boolean rowDeleted) throws Exception {

		Collection<String> columns = getRandomColumns(new Random().nextInt(26) + 1);

		List<TestTokenRange> testRanges = TestUtils.getTestTokenRanges();
		
		Map<String, Integer> expectedRowCounts = rowDeleted ? new HashMap<String, Integer>() : getExpectedRowCountsForTokenRanges(testRanges, columns.size());
		Map<String, Integer> resultRowCounts = new HashMap<String, Integer>();
		
		for (TestTokenRange testRange : testRanges) {
			
			Map<String, Integer> rowCounts = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.withColumnSlice(columns)
					.getColumnCounts()
					.execute().getResult();

			resultRowCounts.putAll(rowCounts);
		}
		
		Assert.assertEquals(expectedRowCounts, resultRowCounts);
	}
	
	private void testColumnCountRowRangeAndColumnRange(boolean rowDeleted) throws Exception {

		// Get the random start column
		int rand = new Random().nextInt(26);
		char randCH = (char) ('a' + rand);
		String startColumn = String.valueOf(randCH);

		int expectedColCount = 'z' - startColumn.charAt(0) + 1; 
		
		List<TestTokenRange> testRanges = TestUtils.getTestTokenRanges();
		
		Map<String, Integer> expectedRowCounts = rowDeleted ? new HashMap<String, Integer>() : getExpectedRowCountsForTokenRanges(testRanges, expectedColCount);
		Map<String, Integer> resultRowCounts = new HashMap<String, Integer>();
		
		for (TestTokenRange testRange : testRanges) {
			
			Map<String, Integer> rowCounts = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.withColumnRange(startColumn, "z", false, -1)
					.getColumnCounts()
					.execute().getResult();

			resultRowCounts.putAll(rowCounts);
		}
		
		Assert.assertEquals(expectedRowCounts, resultRowCounts);
	}

	private void populateRowsForColumnRange() throws Exception {
		
        MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
        	String rowKey = Character.toString(keyName);
        	ColumnListMutation<String> colMutation = m.withRow(CF_COLUMN_RANGE_TEST, rowKey);
              for (char cName = 'a'; cName <= 'z'; cName++) {
            	  colMutation.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
              }
              m.withCaching(true);
              m.execute();
              m.discardMutations();
        }
	}

	private void deleteRowsForColumnRange() throws Exception {
		
        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
            MutationBatch m = keyspace.prepareMutationBatch();
        	String rowKey = Character.toString(keyName);
        	m.withRow(CF_COLUMN_RANGE_TEST, rowKey).delete();
        	m.execute();
        	m.discardMutations();
        }
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
	
	private Map<String, Integer> getExpectedRowCountsForTokenRanges(List<TestTokenRange> testRanges, int expectedColumnCountForEachRow) {
		
		Map<String, Integer> expectedRowCounts = new HashMap<String, Integer>();

		for (TestTokenRange range : testRanges) {
			for (String rowKey : range.expectedRowKeys) {
				expectedRowCounts.put(rowKey, expectedColumnCountForEachRow);
			}
		}
		
		return expectedRowCounts;
	}
}
