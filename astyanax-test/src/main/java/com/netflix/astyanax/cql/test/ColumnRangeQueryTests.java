package com.netflix.astyanax.cql.test;

import java.util.Random;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.util.RangeBuilder;

public class ColumnRangeQueryTests extends ReadTests {

	@BeforeClass
	public static void init() throws Exception {
		initContext();
	}
	
	@Test
	public void testColumnRangeQuery() throws Exception {
		
		//createColumnFamilyForColumnRange();
		//populateRowsForColumnRange();
		
		//readColumnRangeForAllRows();
		
		//paginateColumnsForAllRows();
		
		//getColumnCountForRows(); 
		//getColumnCountForRowKey("A");
	}
	
	public void readColumnRangeForAllRows() throws Exception {
		
		char ch = 'A';
		while (ch <= 'Z') {
			readColumnRangeForRowKey(String.valueOf(ch));
			ch++;
		}
	}

	
	private void readColumnRangeForRowKey(String rowKey) throws Exception {
		
		ColumnList<String> columns = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getKey(rowKey)
				.withColumnRange("a", "z", false, -1)
				.execute().getResult();

		Assert.assertFalse(columns.isEmpty());
		
		char ch = 'a';
		for (Column<String> c : columns) {
			Assert.assertEquals(String.valueOf(ch), c.getName());
			Assert.assertTrue( ch-'a'+1 == c.getIntegerValue());
			ch++;
		}
	}
	
	

	
	public void paginateColumnsForAllRows() throws Exception {
		
		char ch = 'A';
		while (ch <= 'Z') {
			paginateColumnsForRowKey(String.valueOf(ch));
			ch++;
		}
	}

	
	private void paginateColumnsForRowKey(String rowKey) throws Exception {
		ColumnList<String> columns;
		int pageize = 10;
		RowQuery<String, String> query = keyspace
				.prepareQuery(CF_COLUMN_RANGE_TEST)
				.getKey(rowKey)
				.autoPaginate(true)
				.withColumnRange(
						new RangeBuilder().setStart("a")
						.setLimit(pageize).build());

		int count = 1; 
		while (!(columns = query.execute().getResult()).isEmpty()) {
			for (Column<String> col : columns) {
				int value = col.getName().charAt(0) - 'a' + 1;
				Assert.assertEquals(count, value);
				count++;
			}
		}
	}


	private void populateRowsForColumnRange() throws Exception {
		
        MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
        	String rowKey = Character.toString(keyName);
        	ColumnListMutation<String> colMutation = m.withRow(CF_COLUMN_RANGE_TEST, rowKey);
              for (char cName = 'a'; cName <= 'z'; cName++) {
            	  colMutation.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
              }
              m.execute();
        }
        m.discardMutations();
	}
	
	private void createColumnFamilyForColumnRange() throws Exception {
/**		
		CREATE TABLE columnrange (key text, 
								 column1 text,  
								 value int, 
		 						 PRIMARY KEY (key, column1)
				) WITH comment = ''
				
 */

	}
}
