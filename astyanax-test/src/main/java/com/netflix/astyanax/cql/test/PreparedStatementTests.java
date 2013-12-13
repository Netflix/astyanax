package com.netflix.astyanax.cql.test;

import java.util.Date;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.test.utils.ReadTests;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class PreparedStatementTests extends ReadTests {

	private static int TestRowCount = 10;

	private static ColumnFamily<String, String> CF_ROW_RANGE = 
			new ColumnFamily<String, String>("rowrange", StringSerializer.get(), StringSerializer.get(), IntegerSerializer.get());
	
	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_USER_INFO, null);
		keyspace.createColumnFamily(CF_ROW_RANGE, null);
		CF_USER_INFO.describe(keyspace);
		CF_ROW_RANGE.describe(keyspace);
		populateRowsForUserInfo(TestRowCount); 
		populateRowsForColumnRange();
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_USER_INFO);
		keyspace.dropColumnFamily(CF_ROW_RANGE);
	}

	@Test
	public void testSingleRowAllColumnsQuery() throws Exception {

		for (int i=0; i<TestRowCount; i++) {
			ColumnList<String> result = keyspace.prepareQuery(CF_USER_INFO)
					.withCaching(true)
					.getRow("acct_" + i)
					.execute()
					.getResult();
			super.testAllColumnsForRow(result, i);
		}
	}
	
	@Test
	public void testSingleRowSingleColumnQuery() throws Exception {

		for (int i=0; i<TestRowCount; i++) {
			Column<String> result = keyspace.prepareQuery(CF_USER_INFO)
					.withCaching(true)
					.getRow("acct_" + i)
					.getColumn("address")
					.execute()
					.getResult();
			
			Assert.assertNotNull(result);
			Assert.assertEquals("address", result.getName());
			Assert.assertNotNull(result.getStringValue());
			Assert.assertEquals("john smith address " + i, result.getStringValue());
		}
	}
	
	@Test
	public void testSingleRowColumnSliceQueryWithCollection() throws Exception {

		for (int i=0; i<TestRowCount; i++) {
			ColumnList<String> result = keyspace.prepareQuery(CF_USER_INFO)
					.withCaching(true)
					.getRow("acct_" + i)
					.withColumnSlice("firstname", "lastname", "address", "age")
					.execute()
					.getResult();
			
			Assert.assertNotNull(result);
			Assert.assertTrue(4 == result.size());
			Assert.assertEquals("john_" + i, result.getColumnByName("firstname").getStringValue());
			Assert.assertEquals("smith_" + i, result.getColumnByName("lastname").getStringValue());
			Assert.assertEquals("john smith address " + i, result.getColumnByName("address").getStringValue());
			Assert.assertTrue(30 + i == result.getColumnByName("age").getIntegerValue());
		}
	}

	@Test
	public void testRowKeysAllColumnsQuery() throws Exception {

		keyspace.prepareQuery(CF_USER_INFO)
				.withCaching(true)
				.getRowSlice("acct_0", "acct_1", "acct_2", "acct_3")
				.execute();
		
		Rows<String, String> result = keyspace.prepareQuery(CF_USER_INFO)
				.withCaching(true)
				.getRowSlice("acct_4", "acct_5", "acct_6", "acct_7")
				.execute()
				.getResult();

		Assert.assertNotNull(result);
		Assert.assertTrue(4 == result.size());
		for (Row<String, String> row : result) {
			int rowNo = Integer.parseInt(row.getKey().substring("acct_".length()));
			Assert.assertTrue(rowNo >= 4);
			Assert.assertTrue(rowNo <= 7);
			Assert.assertTrue(13 == row.getColumns().size());
		}
	}

	@Test
	public void testRowKeysColumnSetQuery() throws Exception {

		keyspace.prepareQuery(CF_USER_INFO)
				.withCaching(true)
				.getRowSlice("acct_0", "acct_1", "acct_2", "acct_3")
				.withColumnSlice("firstname", "lastname", "age")
				.execute();

		Rows<String, String> result = keyspace.prepareQuery(CF_USER_INFO)
				.withCaching(true)
				.getRowSlice("acct_4", "acct_5", "acct_6", "acct_7")
				.withColumnSlice("firstname", "lastname", "age")
				.execute()
				.getResult();

		Assert.assertNotNull(result);
		Assert.assertTrue(4 == result.size());
		for (Row<String, String> row : result) {
			int rowNo = Integer.parseInt(row.getKey().substring("acct_".length()));
			Assert.assertTrue(rowNo >= 4);
			Assert.assertTrue(rowNo <= 7);
			Assert.assertTrue(3 == row.getColumns().size());
		}
	}

	@Test
	public void testRowKeysColumnRangeQuery() throws Exception {

		keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowSlice("A", "B", "C", "D")
				.withColumnRange(new CqlRangeBuilder<String>()
						.setStart("a")
						.setEnd("c")
						.build())
						.execute();
		
		Rows<String, String> result = keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowSlice("E", "F", "G", "H")
				.withColumnRange(new CqlRangeBuilder<String>()
						.setStart("d")
						.setEnd("h")
						.build())
				.execute()
				.getResult();

		Assert.assertNotNull(result);
		Assert.assertTrue(4 == result.size());
		for (Row<String, String> row : result) {
			int rowNo = row.getKey().charAt(0) - 'A';
			Assert.assertTrue(rowNo >= 4);
			Assert.assertTrue(rowNo <= 7);
			Assert.assertTrue(5 == row.getColumns().size());
		}
	}
	
	@Test
	public void testRowRangeAllColumnsQuery() throws Exception {

		String startToken = Murmur3Partitioner.get().getTokenForKey(StringSerializer.get().fromString("A"));
		String endToken = Murmur3Partitioner.get().getTokenForKey(StringSerializer.get().fromString("G"));
		
		keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowRange(null, null, startToken, endToken, 10)
				.execute();

		Rows<String, String> result = keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowRange(null, null, startToken, endToken, 10)
				.execute()
				.getResult();

		Assert.assertNotNull(result);
		Assert.assertTrue(3 == result.size());
		for (Row<String, String> row : result) {
			Assert.assertTrue(26 == row.getColumns().size());
		}
	}

	@Test
	public void testRowRangeColumnSetQuery() throws Exception {

		String startToken = Murmur3Partitioner.get().getTokenForKey(StringSerializer.get().fromString("A"));
		String endToken = Murmur3Partitioner.get().getTokenForKey(StringSerializer.get().fromString("G"));
		
		keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowRange(null, null, startToken, endToken, 10)
				.withColumnSlice("a", "s", "d", "f")
				.execute();
		
		Rows<String, String> result = keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowRange(null, null, startToken, endToken, 10)
				.withColumnSlice("a", "s", "d", "f")
				.execute()
				.getResult();

		Assert.assertNotNull(result);
		Assert.assertTrue(3 == result.size());
		for (Row<String, String> row : result) {
			Assert.assertTrue(4 == row.getColumns().size());
		}
	}

	@Test
	public void testRowRangeColumnRangeQuery() throws Exception {

		String startToken = Murmur3Partitioner.get().getTokenForKey(StringSerializer.get().fromString("A"));
		String endToken = Murmur3Partitioner.get().getTokenForKey(StringSerializer.get().fromString("G"));
		
		keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowRange(null, null, startToken, endToken, 10)
				.withColumnRange(new CqlRangeBuilder<String>()
						.setStart("d")
						.setEnd("h")
						.build())
						.execute();

		Rows<String, String> result = keyspace.prepareQuery(CF_ROW_RANGE)
				.withCaching(true)
				.getRowRange(null, null, startToken, endToken, 10)
				.withColumnRange(new CqlRangeBuilder<String>()
						.setStart("d")
						.setEnd("h")
						.build())
				.execute()
				.getResult();

		Assert.assertNotNull(result);
		Assert.assertTrue(3 == result.size());
		for (Row<String, String> row : result) {
			Assert.assertTrue(5 == row.getColumns().size());
		}
	}

	private static void populateRowsForUserInfo(int numRows) throws Exception {

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

	private static void populateRowsForColumnRange() throws Exception {
		
        MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
        	String rowKey = Character.toString(keyName);
        	ColumnListMutation<String> colMutation = m.withRow(CF_ROW_RANGE, rowKey);
              for (char cName = 'a'; cName <= 'z'; cName++) {
            	  colMutation.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
              }
              m.withCaching(true);
              m.execute();
              m.discardMutations();
        }
	}

}
