package com.netflix.astyanax.cql.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;

public class CompositeColumnTests extends KeyspaceTests {

	private static AnnotatedCompositeSerializer<Population> compSerializer = new AnnotatedCompositeSerializer<Population>(Population.class);

	private static ColumnFamily<Integer, Population> CF_POPULATION = 
			new ColumnFamily<Integer, Population>("population", IntegerSerializer.get(), compSerializer, IntegerSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_POPULATION,     null);
		CF_POPULATION.describe(keyspace);
	}
	
	@AfterClass
	public static void teardown() throws Exception {
		keyspace.dropColumnFamily(CF_POPULATION);
	}

	@Test
	public void runAllTests() throws Exception {
		
		boolean rowDeleted = false;
		
		populateRowsForCFPopulation();
		Thread.sleep(1000);
		
		/** READ SINGLE ROW QUERIES */
		testReadSingleRowAllColumns(rowDeleted);
		testReadSingleRowSingleColumn(rowDeleted);
		testReadSingleRowColumnRange(rowDeleted);
		
		/** READ ROW SLICE WITH ROW KEYS */
		testReadMultipleRowKeysWithAllColumns(rowDeleted);
		testReadMultipleRowKeysWithColumnRange(rowDeleted);
		
		/** READ ROW SLICE WITH ROWS RANGE */
		testReadRowRangeWithAllColumns(rowDeleted);
		testReadRowRangeWithColumnRange(rowDeleted);
		
		/** ALL ROW COUNT QUERIES */
		testReadSingleRowAllColumnsWithColumnCount(rowDeleted);
		testReadSingleRowColumnRangeWithColumnCount(rowDeleted);
		testReadMultipleRowKeysAllColumnsWithColumnCount(rowDeleted);
		testReadMultipleRowKeysColumnRangeWithColumnCount(rowDeleted);
		testReadRowRangeAllColumnsWithColumnCount(rowDeleted);
		testReadRowRangeColumnRangeWithColumnCount(rowDeleted);

		deleteRowsForCFPopulation(); 
		Thread.sleep(1000);
		rowDeleted = true; 
		
		/** READ SINGLE ROW QUERIES */
		testReadSingleRowAllColumns(rowDeleted);
		testReadSingleRowSingleColumn(rowDeleted);
		testReadSingleRowColumnRange(rowDeleted);
		
		/** READ ROW SLICE WITH ROW KEYS */
		testReadMultipleRowKeysWithAllColumns(rowDeleted);
		testReadMultipleRowKeysWithColumnRange(rowDeleted);
		
		/** READ ROW SLICE WITH ROWS RANGE */
		testReadRowRangeWithAllColumns(rowDeleted);
		testReadRowRangeWithColumnRange(rowDeleted);
		
		/** ALL ROW COUNT QUERIES */
		testReadSingleRowAllColumnsWithColumnCount(rowDeleted);
		testReadSingleRowColumnRangeWithColumnCount(rowDeleted);
		testReadMultipleRowKeysAllColumnsWithColumnCount(rowDeleted);
		testReadMultipleRowKeysColumnRangeWithColumnCount(rowDeleted);
		testReadRowRangeAllColumnsWithColumnCount(rowDeleted);
		testReadRowRangeColumnRangeWithColumnCount(rowDeleted);
	}
	
	private void populateRowsForCFPopulation() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch(); 
		
		Random random = new Random();
		
		for (int year = 2001; year <= 2014; year++) {
			
			m.withRow(CF_POPULATION, year)
				.putColumn(NewYork.clone(), random.nextInt(25000))
				.putColumn(SanDiego.clone(), random.nextInt(25000))
				.putColumn(SanFrancisco.clone(), random.nextInt(25000))
				.putColumn(Seattle.clone(), random.nextInt(25000));
		}
		
		m.execute();
	}

	private void deleteRowsForCFPopulation() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch(); 
		
		for (int year = 2001; year <= 2014; year ++) {
			m.withRow(CF_POPULATION, year).delete();
		}
		
		m.execute();
	}

	private void testReadSingleRowAllColumns(boolean rowDeleted) throws Exception {
		
		for (int year = 2001; year <= 2014; year++) {
			ColumnList<Population> result = keyspace.prepareQuery(CF_POPULATION)
													.getRow(year)
													.execute().getResult();
			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
				continue;
			} else {
				checkResult(result, SanDiego, SanFrancisco, NewYork, Seattle);
			}
		}
	}
	
	private void testReadSingleRowSingleColumn(boolean rowDeleted) throws Exception {
		
		for (int year = 2001; year <= 2014; year++) {

			Column<Population> result = keyspace.prepareQuery(CF_POPULATION)
					.getRow(year)
					.getColumn(SanFrancisco.clone())
					.execute().getResult();
		
			if (rowDeleted) {
				Assert.assertNull(result);
				continue;
			} else {
				Assert.assertTrue(result.hasValue());
			}
		
			Assert.assertEquals(SanFrancisco, result.getName());
		}
	}
	
	private void testReadSingleRowColumnRange(boolean rowDeleted) throws Exception {
		
		AnnotatedCompositeSerializer<Population> compSerializer = new AnnotatedCompositeSerializer<Population>(Population.class);
		
		for (int year = 2001; year <= 2001; year++) {

			ColumnList<Population> result = keyspace.prepareQuery(CF_POPULATION)
					.getRow(year)
					.withColumnRange(compSerializer.buildRange()
									.withPrefix("CA")
									.build())
					.execute().getResult();
		
			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
				continue;
			} else {
				checkResult(result, SanDiego, SanFrancisco);
			}
		
			result = keyspace.prepareQuery(CF_POPULATION)
					.getRow(year)
					.withColumnRange(compSerializer.buildRange()
									.withPrefix("CA")
									.greaterThan("San Diego")
									.build())
					.execute().getResult();
		
			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
				continue;
			} else {
				checkResult(result, SanFrancisco);
			}
			
			result = keyspace.prepareQuery(CF_POPULATION)
					.getRow(year)
					.withColumnRange(compSerializer.buildRange()
									.withPrefix("WA")
									.withPrefix("Seattle")
									.withPrefix(40000)
									.build())
					.execute().getResult();
		
			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
				continue;
			} else {
				checkResult(result, Seattle);
			}
		}
	}
	
	private void testReadMultipleRowKeysWithAllColumns(boolean rowDeleted) throws Exception {
		
		Rows<Integer, Population> result = keyspace.prepareQuery(CF_POPULATION)
				.getKeySlice(2001, 2002, 2003, 2004, 2005)
				.execute().getResult();
		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			checkRowResult(result, 2001, 5, SanDiego, SanFrancisco, NewYork, Seattle);
		}
	}
	
	private void testReadMultipleRowKeysWithColumnRange(boolean rowDeleted) throws Exception {
		
		Rows<Integer, Population> result = keyspace.prepareQuery(CF_POPULATION)
													.getKeySlice(2001, 2002, 2003, 2004, 2005)
													.withColumnRange(compSerializer.buildRange()
															.withPrefix("CA")
															.build())
											.execute().getResult();
								
		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			checkRowResult(result, 2001, 5, SanDiego, SanFrancisco);
		}
								
		result = keyspace.prepareQuery(CF_POPULATION)
						 .getKeySlice(2001, 2002, 2003, 2004, 2005)
						 .withColumnRange(compSerializer.buildRange()
								 		  .withPrefix("CA")
								 		  .greaterThan("San Diego")
								 		  .build())
								 		  .execute().getResult();
								
		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			checkRowResult(result, 2001, 5, SanFrancisco);
		}
									
		result = keyspace.prepareQuery(CF_POPULATION)
				 .getKeySlice(2001, 2002, 2003, 2004, 2005)
				 .withColumnRange(compSerializer.buildRange()
						 		  .withPrefix("WA")
						 		  .withPrefix("Seattle")
						 		  .withPrefix(40000)
						 		  .build())
						 		  .execute().getResult();
								
		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			checkRowResult(result, 2001, 5, Seattle);
		}
	}
	
	private void testReadRowRangeWithAllColumns(boolean rowDeleted) throws Exception {

		List<TestRange> testRanges = getTestRanges();

		for (TestRange testRange : testRanges) {
			Rows<Integer, Population> result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
			} else {
				checkRowResult(result, testRange.expectedRowKeys, SanDiego, SanFrancisco, NewYork, Seattle);
			}
		}
	}

	private void testReadRowRangeWithColumnRange(boolean rowDeleted) throws Exception {

		List<TestRange> testRanges = getTestRanges();
		for (TestRange testRange : testRanges) {

			Rows<Integer, Population> result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.withColumnRange(compSerializer.buildRange()
							.withPrefix("CA")
							.build())
							.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
			} else {
				checkRowResult(result, testRange.expectedRowKeys, SanDiego, SanFrancisco);
			}

			result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.withColumnRange(compSerializer.buildRange()
							.withPrefix("CA")
							.greaterThan("San Diego")
							.build())
							.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
			} else {
				checkRowResult(result, testRange.expectedRowKeys, SanFrancisco);
			}

			result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.withColumnRange(compSerializer.buildRange()
							.withPrefix("WA")
							.withPrefix("Seattle")
							.withPrefix(40000)
							.build())
							.execute().getResult();

			if (rowDeleted) {
				Assert.assertTrue(result.isEmpty());
			} else {
				checkRowResult(result, testRange.expectedRowKeys, Seattle);
			}
		}
	}
	
	/** ALL COLUMN COUNT QUERIES */
	
	private void testReadSingleRowAllColumnsWithColumnCount(boolean rowDeleted) throws Exception {
		
		for (int year = 2001; year <= 2014; year++) {
			Integer result = keyspace.prepareQuery(CF_POPULATION)
													.getRow(year)
													.getCount()
													.execute().getResult();
			int expected = rowDeleted ? 0 : 4;
			Assert.assertTrue(expected == result.intValue());
		}
	}

	private void testReadSingleRowColumnRangeWithColumnCount(boolean rowDeleted) throws Exception {
		
		for (int year = 2001; year <= 2014; year++) {
			
			Integer result = keyspace.prepareQuery(CF_POPULATION)
												   .getRow(year)
												   .withColumnRange(compSerializer.buildRange()
														   			.withPrefix("CA")
														   			.build())
														   			.getCount()
														   			.execute().getResult();
			int expected = rowDeleted ? 0 : 2;
			Assert.assertTrue(expected == result.intValue());

			result = keyspace.prepareQuery(CF_POPULATION)
										  .getRow(year)
										  .withColumnRange(compSerializer.buildRange()
												  		   .withPrefix("CA")
												  		   .greaterThan("San Diego")
												  		   .build())
												  		   .getCount()
												  		   .execute().getResult();
			expected = rowDeleted ? 0 : 1;
			Assert.assertTrue(expected == result.intValue());

			result = keyspace.prepareQuery(CF_POPULATION)
					   						.getRow(year)
					   						.withColumnRange(compSerializer.buildRange()
					   								.withPrefix("WA")
					   								.withPrefix("Seattle")
					   								.withPrefix(40000)
					   								.build())
					   								.getCount()
					   								.execute().getResult();
			expected = rowDeleted ? 0 : 1;
			Assert.assertTrue(expected == result.intValue());
		}
	}

	private void testReadMultipleRowKeysAllColumnsWithColumnCount(boolean rowDeleted) throws Exception {

		Map<Integer, Integer> result = keyspace.prepareQuery(CF_POPULATION)
				.getKeySlice(2001, 2002, 2003, 2004, 2005)
				.getColumnCounts()
				.execute().getResult();
		Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
		if (!rowDeleted) {
			for (int year = 2001; year<= 2005; year++) {
				expected.put(year, 4);
			}
		}
		Assert.assertEquals(expected, result);
	}

	private void testReadMultipleRowKeysColumnRangeWithColumnCount(boolean rowDeleted) throws Exception {

			Map<Integer, Integer> result = keyspace.prepareQuery(CF_POPULATION)
					 .getKeySlice(2001, 2002, 2003, 2004, 2005)
					 .withColumnRange(compSerializer.buildRange()
							.withPrefix("CA")
							.build())
							.getColumnCounts()
							.execute().getResult();

			Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
			if (!rowDeleted) {
				for (Integer rowKey = 2001; rowKey<=2005; rowKey++) {
					expected.put(rowKey, 2);
				}
			}
			
			Assert.assertEquals(expected, result);
			
			result = keyspace.prepareQuery(CF_POPULATION)
					 .getKeySlice(2001, 2002, 2003, 2004, 2005)
					 .withColumnRange(compSerializer.buildRange()
							.withPrefix("CA")
							.greaterThan("San Diego")
							.build())
							.getColumnCounts()
							.execute().getResult();

			expected = new HashMap<Integer, Integer>();
			if (!rowDeleted) {
				for (Integer rowKey = 2001; rowKey<=2005; rowKey++) {
					expected.put(rowKey, 1);
				}
			}
			
			Assert.assertEquals(expected, result);
			
			result = keyspace.prepareQuery(CF_POPULATION)
					 .getKeySlice(2001, 2002, 2003, 2004, 2005)
					 .withColumnRange(compSerializer.buildRange()
							.withPrefix("WA")
							.withPrefix("Seattle")
							.withPrefix(40000)
							.build())
							.getColumnCounts()
							.execute().getResult();

			expected = new HashMap<Integer, Integer>();
			if (!rowDeleted) {
				for (Integer rowKey = 2001; rowKey<=2005; rowKey++) {
					expected.put(rowKey, 1);
				}
			}
			
			Assert.assertEquals(expected, result);
	}
	
	private void testReadRowRangeAllColumnsWithColumnCount(boolean rowDeleted) throws Exception {

		List<TestRange> testRanges = getTestRanges();
		
		TestRange range = testRanges.get(0);
		
		Map<Integer, Integer> result = keyspace.prepareQuery(CF_POPULATION)
				.getKeyRange(null, null, range.start, range.end, 100)
				.getColumnCounts()
				.execute().getResult();
		
		Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
		if (!rowDeleted) {
			for (Integer year : range.expectedRowKeys) {
				expected.put(year, 4);
			}
		}
		Assert.assertEquals(expected, result);
	}

	private void testReadRowRangeColumnRangeWithColumnCount(boolean rowDeleted) throws Exception {

		List<TestRange> testRanges = getTestRanges();
		for (TestRange testRange : testRanges) {

			Map<Integer, Integer> result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.withColumnRange(compSerializer.buildRange()
							.withPrefix("CA")
							.build())
							.getColumnCounts()
							.execute().getResult();

			Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
			if (!rowDeleted) {
				for (Integer rowKey : testRange.expectedRowKeys) {
					expected.put(rowKey, 2);
				}
			}
			
			Assert.assertEquals(expected, result);
			
			result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.withColumnRange(compSerializer.buildRange()
							.withPrefix("CA")
							.greaterThan("San Diego")
							.build())
							.getColumnCounts()
							.execute().getResult();

			expected = new HashMap<Integer, Integer>();
			if (!rowDeleted) {
				for (Integer rowKey : testRange.expectedRowKeys) {
					expected.put(rowKey, 1);
				}
			}
			
			Assert.assertEquals(expected, result);
			
			result = keyspace.prepareQuery(CF_POPULATION)
					.getKeyRange(null, null, testRange.start, testRange.end, 100)
					.withColumnRange(compSerializer.buildRange()
							.withPrefix("WA")
							.withPrefix("Seattle")
							.withPrefix(40000)
							.build())
							.getColumnCounts()
							.execute().getResult();

			expected = new HashMap<Integer, Integer>();
			if (!rowDeleted) {
				for (Integer rowKey : testRange.expectedRowKeys) {
					expected.put(rowKey, 1);
				}
			}
			
			Assert.assertEquals(expected, result);
		}
	}
	
	private void checkResult(ColumnList<Population> result,  Population ... expected) throws Exception {
		
		Assert.assertFalse(result.isEmpty());
		Assert.assertEquals(expected.length, result.size());
		int index = 0;
		for (Population p : expected) {
			Assert.assertEquals(p, result.getColumnByIndex(index++).getName());
		}
	}

	private void checkRowResult(Rows<Integer, Population> result, Integer startKey, Integer size, Population ... expected) throws Exception {
		
		int rowKey = startKey;
		for (Row<Integer, Population> row : result) {
			Assert.assertTrue(rowKey == row.getKey());
			checkResult(row.getColumns(), expected);
			rowKey++;
		}
		Assert.assertTrue("Result: " + result.size() + ", size: " + size, size == result.size());
	}
	
	private void checkRowResult(Rows<Integer, Population> result, List<Integer> rowKeys, Population ... expected) throws Exception {
		
		int index = 0;
		for (Row<Integer, Population> row : result) {
			Assert.assertEquals(rowKeys.toString() + " " + row.getKey(), rowKeys.get(index++), row.getKey());
			checkResult(row.getColumns(), expected);
		}
		Assert.assertTrue(rowKeys.size() == result.size());
	}
	
	/** TEST CITIES */
	public static Population NewYork = new Population("NY", "New York", 10000);
	public static Population SanDiego = new Population("CA", "San Diego", 20000);
	public static Population SanFrancisco = new Population("CA", "San Francisco", 30000);
	public static Population Seattle = new Population("WA", "Seattle", 40000);

	public static class Population {
		
		@Component(ordinal=0) String state;
		@Component(ordinal=1) String city;
		@Component(ordinal=2) Integer zipcode;
		
		public Population() {
		}

		public Population(String state, String city, Integer zipcode) {
			this.state = state;
			this.city = city;
			this.zipcode = zipcode;
		}

		public String toString() {
			return "Population [" + state + ", " + city + ", " + zipcode + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((state == null) ? 0 : state.hashCode());
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result + ((zipcode == null) ? 0 : zipcode.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null)return false;
			if (getClass() != obj.getClass()) return false;
			Population other = (Population) obj;
			boolean equal = true;
			equal &= (state != null) ? (state.equals(other.state)) : other.state == null; 
			equal &= (city != null) ? (city.equals(other.city)) : other.city == null; 
			equal &= (zipcode != null) ? (zipcode.equals(other.zipcode)) : other.zipcode == null;
			return equal;
		}
		
		public Population clone() {
			return new Population(state, city, zipcode);
		}
	}	
	
	/**
	 *   2014 -->  -6625834866172541556    2003 -->  -5952676706262623311    2009 -->  -4850296245464368619
	 *   2010 -->  -4012971246572234480    2005 -->  -3904377230599730913    2006 -->  -3604768136712843506 
	 *   2012 -->  -3193851331505022123    2007 -->  -797272529921810676     2001 -->   267648259961407629 
	 *   2002 -->   313927025611477591     2011 -->   2700799408278278395    2004 -->   5455601112738248795  
	 *   2013 -->   8821734684824899422    2008 -->   9033513988054576353
	*/
	
	private static class TestRange {
		
		private String start; 
		private String end; 
		private List<Integer> expectedRowKeys = new ArrayList<Integer>(); 
		
		private TestRange(String start, String end, Integer ... rows) {
			this.start = start;
			this.end = end;
			this.expectedRowKeys.addAll(Arrays.asList(rows));
		}
	}
	
	private List<TestRange> getTestRanges() {
		
		List<TestRange> list = new ArrayList<TestRange>();
		list.add(new TestRange("-6625834866172541556", "-4850296245464368619", 2014, 2003, 2009));
		list.add(new TestRange("-4012971246572234480", "-3604768136712843506", 2010, 2005, 2006));
		list.add(new TestRange("-3193851331505022123", "267648259961407629", 2012, 2007, 2001));
		list.add(new TestRange("313927025611477591", "5455601112738248795", 2002, 2011, 2004));
		list.add(new TestRange("8821734684824899422", "9033513988054576353", 2013, 2008));
		return list;
	}
}
