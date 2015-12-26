package com.netflix.astyanax.cql.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.reads.model.CqlRangeImpl;
import com.netflix.astyanax.cql.test.utils.ReadTests;
import com.netflix.astyanax.cql.test.utils.TestUtils;
import com.netflix.astyanax.cql.test.utils.TestUtils.TestTokenRange;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;


public class RowSliceRowRangeQueryTests extends ReadTests {

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
		
		boolean rowDeleted = false; 
		
		TestUtils.populateRowsForColumnRange(keyspace);
		Thread.sleep(1000);
		
		testRowKeysWithAllColumns(rowDeleted);
		testRowKeysWithColumnSet(rowDeleted);
		testRowKeysWithColumnRange(rowDeleted);
		
		testRowRangeWithAllColumns(rowDeleted);
		testRowRangeWithColumnSet(rowDeleted);
		testRowRangeWithColumnRange(rowDeleted);
		
		TestUtils.deleteRowsForColumnRange(keyspace);
		Thread.sleep(1000);
		rowDeleted = true; 
		
		testRowKeysWithAllColumns(rowDeleted);
		testRowKeysWithColumnSet(rowDeleted);
		testRowKeysWithColumnRange(rowDeleted);
		
		testRowRangeWithAllColumns(rowDeleted);
		testRowRangeWithColumnSet(rowDeleted);
		testRowRangeWithColumnRange(rowDeleted);
	}
	
	private void testRowKeysWithAllColumns(boolean rowDeleted) throws Exception {
		
		Set<String> rowKeys = getRandomRowKeys();
		
		Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST).getRowSlice(rowKeys).execute().getResult();
    	
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
			return;
		}
		
    	Assert.assertFalse(rows.isEmpty());

    	int rowKeysSize = rowKeys.size();
    	for (Row<String, String> row : rows) {
    		
    		boolean isPresent = rowKeys.remove(row.getKey());
    		Assert.assertTrue("Extraneous row: " + row.getKey(), isPresent);
    		
        	ColumnList<String> colList = row.getColumns();
        	Assert.assertEquals(26, colList.size());
        	for(int index=0; index<26; index++) { 
        		Column<String> col = colList.getColumnByIndex(index);
        		Assert.assertTrue(String.valueOf((char)('a' + index)).equals(col.getName()));
        		Assert.assertEquals(index + 1, col.getIntegerValue());
        	}
    	}
    	Assert.assertEquals(rowKeysSize, rows.size());
	}
	

	private void testRowKeysWithColumnSet(boolean rowDeleted) throws Exception {
		
		Set<String> rowKeys = getRandomRowKeys();
		Set<String> columns = getRandomColumns();
		
		Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
											.getRowSlice(rowKeys)
											.withColumnSlice(columns)
											.execute().getResult();
		
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
			return;
		}
		
    	Assert.assertFalse(rows.isEmpty());

    	List<String> expected = new ArrayList<String>(columns);
    	Collections.sort(expected);
    
    	int rowKeysSize = rowKeys.size();
    	for (Row<String, String> row : rows) {
    		
    		boolean isPresent = rowKeys.remove(row.getKey());
    		Assert.assertTrue("Extraneous row: " + row.getKey(), isPresent);
    		
        	List<String> result = new ArrayList<String>();
        	ColumnList<String> colList = row.getColumns();
        	for (Column<String> col : colList) {
        		result.add(col.getName());
        	}
        	Collections.sort(result);
        	
        	Assert.assertEquals(expected, result);
    	}
    	Assert.assertEquals(rowKeysSize, rows.size());
	}
	
	@SuppressWarnings("unchecked")
	private void testRowKeysWithColumnRange(boolean rowDeleted) throws Exception {
		
		Set<String> rowKeys = getRandomRowKeys();
		
		// get random start and end column
		CqlRangeImpl<String> columns = (CqlRangeImpl<String>) getRandomColumnRange();
		
		Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
											.getRowSlice(rowKeys)
											.withColumnRange(columns)
											.execute().getResult();
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
			return;
		}
		
    	Assert.assertFalse(rows.isEmpty());

    	int rowKeysSize = rowKeys.size();
    	
    	for (Row<String, String> row : rows) {
    		
    		boolean isPresent = rowKeys.remove(row.getKey());
    		Assert.assertTrue("Extraneous row: " + row.getKey(), isPresent);
    		
    		int numExpectedCols = columns.getCqlEnd().charAt(0) - columns.getCqlStart().charAt(0) + 1;
    		
        	ColumnList<String> colList = row.getColumns();
        	Assert.assertEquals(numExpectedCols, colList.size());

        	for (Column<String> col : colList) {
        		Assert.assertTrue(col.getName().compareTo(columns.getCqlStart()) >= 0);
        		Assert.assertTrue(col.getName().compareTo(columns.getCqlEnd()) <= 0);
        	}
    	}
    	Assert.assertEquals(rowKeysSize, rows.size());
	}

	
	private void testRowRangeWithAllColumns(boolean rowDeleted) throws Exception {

		List<String> expectedColumns = new ArrayList<String>();
		for (char ch = 'a'; ch <= 'z'; ch++) {
			expectedColumns.add(String.valueOf(ch));
		}
		
		for (TestTokenRange testRange : getTestTokenRanges()) {
		
			Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(rows.isEmpty());
				continue;
			}

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

	private void testRowRangeWithColumnSet(boolean rowDeleted) throws Exception {

		Set<String> randomColumns = getRandomColumns();
		
		List<String> expectedColumns = new ArrayList<String>(randomColumns);
		Collections.sort(expectedColumns);
		
		for (TestTokenRange testRange : getTestTokenRanges()) {
		
			Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.withColumnSlice(randomColumns)
					.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(rows.isEmpty());
				continue;
			}

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
	
	private void testRowRangeWithColumnRange(boolean rowDeleted) throws Exception {

		CqlRangeImpl<String> columnRange = (CqlRangeImpl<String>) getRandomColumnRange();
		
		for (TestTokenRange testRange : getTestTokenRanges()) {
		
			Rows<String, String> rows = keyspace.prepareQuery(CF_COLUMN_RANGE_TEST)
					.getRowRange(null, null, testRange.startToken, testRange.endToken, -1)
					.withColumnRange(columnRange)
					.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(rows.isEmpty());
				continue;
			}

			Assert.assertFalse(rows.isEmpty());
			
    		int numExpectedCols = columnRange.getCqlEnd().charAt(0) - columnRange.getCqlStart().charAt(0) + 1;
    		

			List<String> list = new ArrayList<String>();
			for (Row<String, String> row : rows) {
				String key = row.getKey();
				list.add(key);
				
	        	ColumnList<String> colList = row.getColumns();
	        	Assert.assertEquals(numExpectedCols, colList.size());

	        	for (Column<String> col : colList) {
	        		Assert.assertTrue(col.getName().compareTo(columnRange.getCqlStart()) >= 0);
	        		Assert.assertTrue(col.getName().compareTo(columnRange.getCqlEnd()) <= 0);
	        	}
			}
			
			Assert.assertEquals(testRange.expectedRowKeys, list);
		}
	}
	
	private Set<String> getRandomRowKeys() {
		
		Random random = new Random();
		int numRowKeys = random.nextInt(26) + 1;  // avoid 0 rows
		
		Set<String> set = new HashSet<String>();
		for (int i=0; i<numRowKeys; i++) {
			
			int no = random.nextInt(26);
			char ch = (char) ('A' + no);
			set.add(String.valueOf(ch));
		}
		System.out.println("Set: " + set);
		return set;
	}
	
	private Set<String> getRandomColumns() {
		
		Random random = new Random();
		int numRowKeys = random.nextInt(26) + 1;  // avoid 0 rows
		
		Set<String> set = new HashSet<String>();
		for (int i=0; i<numRowKeys; i++) {
			
			int no = random.nextInt(26);
			char ch = (char) ('a' + no);
			set.add(String.valueOf(ch));
		}
		
		return set;
	}
	
	private ByteBufferRange getRandomColumnRange() {
		
		Random random = new Random();
		Integer n1 = random.nextInt(26);
		Integer n2 = random.nextInt(26);
		
		String c1 = String.valueOf((char)('a' + n1));
		String c2 = String.valueOf((char)('a' + n2));
		
		if (n1 < n2) {
			return new CqlRangeBuilder<String>().setStart(c1).setEnd(c2).build();
		} else {
			return new CqlRangeBuilder<String>().setStart(c2).setEnd(c1).build();
		}
	}
	
	private List<TestTokenRange> getTestTokenRanges() {
		return TestUtils.getTestTokenRanges();
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

	

}
