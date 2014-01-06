package com.netflix.astyanax.cql.test;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class StaticColumnFamilyTests extends KeyspaceTests {

	private static ColumnFamily<String, String> CF_ACCOUNTS = new ColumnFamily<String, String>("accounts", StringSerializer.get(), StringSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.prepareQuery(CF_ACCOUNTS)
				.withCql("CREATE TABLE astyanaxunittests.accounts (userid text PRIMARY KEY, user text, pswd text)")
				.execute();
		CF_ACCOUNTS.describe(keyspace);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_ACCOUNTS);
	}

	@Test
	public void testReadWriteOpsWithStaticNamedColumns() throws Exception {

		populateRowsForAccountsTable(keyspace);
		Thread.sleep(200);
		boolean rowDeleted = false; 

		performSimpleRowQuery(rowDeleted);
		performSimpleRowQueryWithColumnCollection(rowDeleted);
		performSimpleRowSingleColumnQuery(rowDeleted);
		performRowSliceQueryWithAllColumns(rowDeleted);
		performRowSliceQueryWithColumnSlice(rowDeleted);

		deleteRowsForAccountsTable(keyspace);
		Thread.sleep(200);
		rowDeleted = true; 

		performSimpleRowQuery(rowDeleted);
		performSimpleRowQueryWithColumnCollection(rowDeleted);
		performSimpleRowSingleColumnQuery(rowDeleted);
		performRowSliceQueryWithAllColumns(rowDeleted);
		performRowSliceQueryWithColumnSlice(rowDeleted);
	}


	private void performSimpleRowQuery(boolean rowDeleted) throws Exception {
		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			String key = Character.toString(keyName);
			performSimpleRowQueryForRow(key, rowDeleted, key);
		}
	}

	private void performSimpleRowQueryForRow(String rowKey, boolean rowDeleted, String expectedChar) throws Exception {

		ColumnList<String> result =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			Assert.assertFalse(result.isEmpty());
			Column<String> col = result.getColumnByName("user");
			Assert.assertEquals("user" + expectedChar, col.getStringValue());
			col = result.getColumnByName("pswd");
			Assert.assertEquals("pswd" + expectedChar, col.getStringValue());
		}
	}

	private void performSimpleRowQueryWithColumnCollection(boolean rowDeleted) throws Exception {
		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			String key = Character.toString(keyName);
			performSimpleRowQueryWithColumnCollectionForRow(key, rowDeleted, key);
		}
	}

	private void performSimpleRowQueryWithColumnCollectionForRow(String rowKey, boolean rowDeleted, String expectedChar) throws Exception {

		ColumnList<String> result =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).withColumnSlice("user", "pswd").execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			Assert.assertFalse(result.isEmpty());
			Column<String> col = result.getColumnByName("user");
			Assert.assertEquals("user" + expectedChar, col.getStringValue());
			col = result.getColumnByName("pswd");
			Assert.assertEquals("pswd" + expectedChar, col.getStringValue());
		}

		result =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).withColumnSlice("user").execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			Assert.assertFalse(result.isEmpty());
			Column<String> col = result.getColumnByName("user");
			Assert.assertEquals("user" + expectedChar, col.getStringValue());
		}

		result =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).withColumnSlice("pswd").execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			Assert.assertFalse(result.isEmpty());
			Column<String> col = result.getColumnByName("pswd");
			Assert.assertEquals("pswd" + expectedChar, col.getStringValue());
		}

		List<String> cols = new ArrayList<String>();
		cols.add("user"); cols.add("pswd");

		result =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).withColumnSlice(cols).execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			Assert.assertFalse(result.isEmpty());
			Column<String> col = result.getColumnByName("user");
			Assert.assertEquals("user" + expectedChar, col.getStringValue());
			col = result.getColumnByName("pswd");
			Assert.assertEquals("pswd" + expectedChar, col.getStringValue());
		}

		cols.remove("user");

		result =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).withColumnSlice(cols).execute().getResult();

		if (rowDeleted) {
			Assert.assertTrue(result.isEmpty());
		} else {
			Assert.assertFalse(result.isEmpty());
			Column<String> col = result.getColumnByName("pswd");
			Assert.assertEquals("pswd" + expectedChar, col.getStringValue());
		}
	}

	private void performSimpleRowSingleColumnQuery(boolean rowDeleted) throws Exception {
		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			String key = Character.toString(keyName);
			performSimpleRowSingleColumnQueryForRow(key, rowDeleted, key);
		}
	}

	private void performSimpleRowSingleColumnQueryForRow(String rowKey, boolean rowDeleted, String expectedChar) throws Exception {

		Column<String> col =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).getColumn("user").execute().getResult();
		if (rowDeleted) {
			Assert.assertNull(col);
		} else {
			Assert.assertTrue(col.hasValue());
			Assert.assertEquals("user" + expectedChar, col.getStringValue());
		}

		col =  keyspace.prepareQuery(CF_ACCOUNTS).getRow(rowKey).getColumn("pswd").execute().getResult();
		if (rowDeleted) {
			Assert.assertNull(col);
		} else {
			Assert.assertTrue(col.hasValue());
			Assert.assertEquals("pswd" + expectedChar, col.getStringValue());
		}
	}

	private void performRowSliceQueryWithAllColumns(boolean rowDeleted) throws Exception {

		List<String> keys = new ArrayList<String>();
		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			keys.add(Character.toString(keyName));
		}

		int index = 0;
		Rows<String, String> rows =  keyspace.prepareQuery(CF_ACCOUNTS).getRowSlice(keys).execute().getResult();
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
		} else {
			Assert.assertFalse(rows.isEmpty());
			for (com.netflix.astyanax.model.Row<String, String> row : rows) {

				Assert.assertEquals(keys.get(index),row.getKey());

				ColumnList<String> cols = row.getColumns();
				Assert.assertFalse(cols.isEmpty());
				Column<String> col = cols.getColumnByName("user");
				Assert.assertEquals("user" + keys.get(index), col.getStringValue());
				col = cols.getColumnByName("pswd");
				Assert.assertEquals("pswd" + keys.get(index), col.getStringValue());

				index++;
			}
		}
	}

	private void performRowSliceQueryWithColumnSlice(boolean rowDeleted) throws Exception {

		List<String> keys = new ArrayList<String>();
		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			keys.add(Character.toString(keyName));
		}

		int index = 0;
		Rows<String, String> rows =  keyspace.prepareQuery(CF_ACCOUNTS).getRowSlice(keys).withColumnSlice("user", "pswd").execute().getResult();
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
		} else {
			Assert.assertFalse(rows.isEmpty());
			for (com.netflix.astyanax.model.Row<String, String> row : rows) {

				Assert.assertEquals(keys.get(index),row.getKey());

				ColumnList<String> cols = row.getColumns();
				Assert.assertFalse(cols.isEmpty());
				Column<String> col = cols.getColumnByName("user");
				Assert.assertEquals("user" + keys.get(index), col.getStringValue());
				col = cols.getColumnByName("pswd");
				Assert.assertEquals("pswd" + keys.get(index), col.getStringValue());

				index++;
			}
		}

		index=0;
		rows =  keyspace.prepareQuery(CF_ACCOUNTS).getRowSlice(keys).withColumnSlice("user").execute().getResult();
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
		} else {
			Assert.assertFalse(rows.isEmpty());
			for (com.netflix.astyanax.model.Row<String, String> row : rows) {

				Assert.assertEquals(keys.get(index),row.getKey());

				ColumnList<String> cols = row.getColumns();
				Assert.assertFalse(cols.isEmpty());
				Column<String> col = cols.getColumnByName("user");
				Assert.assertEquals("user" + keys.get(index), col.getStringValue());

				index++;
			}
		}

		index=0;
		rows =  keyspace.prepareQuery(CF_ACCOUNTS).getRowSlice(keys).withColumnSlice("pswd").execute().getResult();
		if (rowDeleted) {
			Assert.assertTrue(rows.isEmpty());
		} else {
			Assert.assertFalse(rows.isEmpty());
			for (com.netflix.astyanax.model.Row<String, String> row : rows) {

				Assert.assertEquals(keys.get(index),row.getKey());

				ColumnList<String> cols = row.getColumns();
				Assert.assertFalse(cols.isEmpty());
				Column<String> col = cols.getColumnByName("pswd");
				Assert.assertEquals("pswd" + keys.get(index), col.getStringValue());

				index++;
			}
		}
	}


	public static void populateRowsForAccountsTable(Keyspace keyspace) throws Exception {

		MutationBatch m = keyspace.prepareMutationBatch();

		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			String character = Character.toString(keyName);
			ColumnListMutation<String> colMutation = m.withRow(CF_ACCOUNTS, character);
			colMutation.putColumn("user", "user" + character).putColumn("pswd", "pswd" + character);
			m.execute();
			m.discardMutations();
		}
	}

	public static void deleteRowsForAccountsTable(Keyspace keyspace) throws Exception {

		for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
			MutationBatch m = keyspace.prepareMutationBatch();
			String rowKey = Character.toString(keyName);
			m.withRow(CF_ACCOUNTS, rowKey).delete();
			m.execute();
			m.discardMutations();
		}
	}
}
