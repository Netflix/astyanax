package com.netflix.astyanax.cql.test;

import java.util.Random;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.test.utils.ReadTests;
import com.netflix.astyanax.cql.test.utils.TestUtils;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

public class SingleRowColumnPaginationTests extends ReadTests {
	
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
		
		paginateColumnsForAllRows(rowDeleted);

		TestUtils.deleteRowsForColumnRange(keyspace);
		Thread.sleep(1000);
		rowDeleted = true;

		paginateColumnsForAllRows(rowDeleted);
	}
	
	private void paginateColumnsForAllRows(boolean rowDeleted) throws Exception {
		
		Random random = new Random();
		
		char ch = 'A';
		while (ch <= 'Z') {
			int pageSize = random.nextInt(26) % 10;
			if (pageSize <= 0) {
				pageSize = 10;
			}
			paginateColumnsForRowKey(String.valueOf(ch), rowDeleted, pageSize);
			ch++;
		}
	}

	private void paginateColumnsForRowKey(String rowKey, boolean rowDeleted, int pageSize) throws Exception {
		ColumnList<String> columns;
		
		RowQuery<String, String> query = keyspace
				.prepareQuery(TestUtils.CF_COLUMN_RANGE_TEST)
				.getKey(rowKey)
				.autoPaginate(true)
				.withColumnRange(
						new CqlRangeBuilder<String>().setStart("a")
						.setFetchSize(pageSize).build());

		int count = 1; 
		while (!(columns = query.execute().getResult()).isEmpty()) {
			Assert.assertTrue(columns.size() <= pageSize);
			
			for (Column<String> col : columns) {
				int value = col.getName().charAt(0) - 'a' + 1;
				Assert.assertEquals(count, value);
				count++;
			}
		}
		
		if (rowDeleted) {
			Assert.assertTrue(count == 1);
		}
	}

}
