/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.cql.test.utils.ReadTests;
import com.netflix.astyanax.cql.test.utils.TestUtils;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;

public class SingleRowColumnRangeQueryTests extends ReadTests {

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
	public void testColumnRangeQuery() throws Exception {
		
		/** POPULATE DATA FOR TESTING */ 
		TestUtils.populateRowsForColumnRange(keyspace);
		Thread.sleep(1000);
		boolean rowDeleted = false;
		
		/** PERFORM READ TESTS */
		readColumnRangeForAllRows(rowDeleted);
		getColumnCountForAllRows(rowDeleted); 
		
		/** DELETE ALL ROWS */ 
		TestUtils.deleteRowsForColumnRange(keyspace);
		rowDeleted = true;

		/** PERFORM READ TESTS FOR MISSING DATA */
		readColumnRangeForAllRows(rowDeleted);
		getColumnCountForAllRows(rowDeleted); 
	}
	
	public void readColumnRangeForAllRows(boolean rowDeleted) throws Exception {
		
		char ch = 'A';
		while (ch <= 'Z') {
			readColumnRangeForRowKey(String.valueOf(ch), rowDeleted);
			ch++;
		}
	}

	private void readColumnRangeForRowKey(String rowKey, boolean rowDeleted) throws Exception {
		
		ColumnList<String> columns = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getKey(rowKey)
				.withColumnRange("a", "z", false, -1)
				.execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(columns.isEmpty());
			return;
		}
		
		Assert.assertFalse(columns.isEmpty());
		
		char ch = 'a';
		for (Column<String> c : columns) {
			Assert.assertEquals(String.valueOf(ch), c.getName());
			Assert.assertTrue( ch-'a'+1 == c.getIntegerValue());
			ch++;
		}
	}
	
	public void getColumnCountForAllRows(boolean rowDeleted) throws Exception {
		
		char ch = 'A';
		while (ch <= 'Z') {
			getColumnCountForRowKey(String.valueOf(ch), rowDeleted);
			ch++;
		}
	}

	private void getColumnCountForRowKey(String rowKey, boolean rowDeleted) throws Exception {
		
		Integer count = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getKey(rowKey)
				.withColumnRange("a", "z", false, -1)
				.getCount()
				.execute().getResult();

		int expectedCount = rowDeleted ? 0 : 26; 
		Assert.assertTrue(count.intValue() == expectedCount);
	}
}
